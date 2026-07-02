// Raft client in Go — demonstrates gRPC interoperability with the
// Python servers. It only uses the application-level operations
// (Publish and Consume, from client.proto); it never touches the
// internal Raft RPCs (RequestVote, AppendEntries, ...).
//
// Leader discovery: the client does NOT know the leader upfront. It
// sends the write to any node; a non-leader answers success=false with
// a leader_id hint, and the client redirects itself.
package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "raftclient/pb"
)

const (
	publishTimeout = 8 * time.Second // Publish blocks server-side until commit
	readTimeout    = 2 * time.Second
)

func serverAddrs() []string {
	env := os.Getenv("SERVERS")
	if env == "" {
		env = "raft-0:50051,raft-1:50051,raft-2:50051,raft-3:50051"
	}
	return strings.Split(env, ",")
}

type Client struct {
	addrs  []string
	stubs  map[int]pb.ClientAPIClient
	leader int // current best guess; refined by redirect hints
}

func NewClient(addrs []string) *Client {
	return &Client{
		addrs:  addrs,
		stubs:  map[int]pb.ClientAPIClient{},
		leader: rand.Intn(len(addrs)), // start at a random node: no prior knowledge
	}
}

func (c *Client) stub(id int) (pb.ClientAPIClient, error) {
	if s, ok := c.stubs[id]; ok {
		return s, nil
	}
	conn, err := grpc.NewClient(
		c.addrs[id],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	s := pb.NewClientAPIClient(conn)
	c.stubs[id] = s
	return s, nil
}

// Publish sends a value to the cluster, following leader hints until it
// reaches the leader. Returns only after the entry is committed.
func (c *Client) Publish(value string) error {
	target := c.leader
	for attempt := 0; attempt < 2*len(c.addrs); attempt++ {
		s, err := c.stub(target)
		if err != nil {
			target = (target + 1) % len(c.addrs)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
		resp, err := s.Publish(ctx, &pb.PublishRequest{Value: value})
		cancel()
		if err != nil {
			fmt.Printf("  n%d unreachable (%s), trying next node\n",
				target, status.Convert(err).Code())
			target = (target + 1) % len(c.addrs)
			continue
		}
		if resp.Success {
			c.leader = target
			fmt.Printf("  published %q via leader n%d (committed)\n", value, target)
			return nil
		}
		hint := int(resp.LeaderId)
		if hint == target {
			// the leader itself rejected: quorum unavailable
			return fmt.Errorf("write %q rejected by leader n%d (no quorum)", value, target)
		}
		fmt.Printf("  n%d is not the leader, redirected to n%d\n", target, hint)
		target = hint
	}
	return fmt.Errorf("could not publish %q: no leader found", value)
}

// Read fetches the committed entries. node == -1 means "any reachable
// node"; otherwise it reads from that specific node (leader or replica).
func (c *Client) Read(node int) error {
	var order []int
	if node >= 0 {
		order = []int{node}
	} else {
		start := rand.Intn(len(c.addrs))
		for i := range c.addrs {
			order = append(order, (start+i)%len(c.addrs))
		}
	}
	for _, id := range order {
		s, err := c.stub(id)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
		resp, err := s.Consume(ctx, &pb.ConsumeRequest{})
		cancel()
		if err != nil {
			fmt.Printf("  n%d unreachable (%s)\n", id, status.Convert(err).Code())
			continue
		}
		fmt.Printf("committed entries read from n%d (%d total):\n", id, len(resp.Entries))
		for i, e := range resp.Entries {
			fmt.Printf("  [%d] %q @t%d\n", i, e.Value, e.Term)
		}
		return nil
	}
	return fmt.Errorf("no node reachable")
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  client                                interactive mode (publish/read/exit)
  client publish <value> [<value>...]   publish values to the cluster
  client read [node]                    read committed entries (from a
                                        specific node, or any if omitted)

env:
  SERVERS  comma-separated node addresses, indexed by node id
           (default: raft-0:50051,...,raft-3:50051)
`)
	os.Exit(2)
}

// repl keeps the client alive between commands, so the leader hint
// learned on the first publish is reused by the following ones.
func repl(c *Client) {
	fmt.Println("interactive mode — commands: publish <v>..., read [node], exit")
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			fmt.Println()
			return
		}
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "publish", "p":
			if len(fields) < 2 {
				fmt.Println("usage: publish <value>...")
				continue
			}
			for _, v := range fields[1:] {
				if err := c.Publish(v); err != nil {
					fmt.Println("error:", err)
				}
			}
		case "read", "r":
			node := -1
			if len(fields) > 1 {
				n, err := strconv.Atoi(fields[1])
				if err != nil {
					fmt.Println("usage: read [node]")
					continue
				}
				node = n
			}
			if err := c.Read(node); err != nil {
				fmt.Println("error:", err)
			}
		case "exit", "quit", "q":
			return
		default:
			fmt.Println("commands: publish <v>..., read [node], exit")
		}
	}
}

func main() {
	c := NewClient(serverAddrs())
	if len(os.Args) < 2 {
		repl(c)
		return
	}

	switch os.Args[1] {
	case "publish":
		if len(os.Args) < 3 {
			usage()
		}
		for _, v := range os.Args[2:] {
			if err := c.Publish(v); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				os.Exit(1)
			}
		}
	case "read":
		node := -1
		if len(os.Args) > 2 {
			n, err := strconv.Atoi(os.Args[2])
			if err != nil {
				usage()
			}
			node = n
		}
		if err := c.Read(node); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
	}
}
