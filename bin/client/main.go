package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/filedag-project/trans"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var logger = logging.Logger("client")

func main() {
	local := []*cli.Command{
		addCmd,
		catCmd,
	}

	app := &cli.App{
		Name: "client",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "level, L",
				Usage: "specify minimum log level, INFO by default",
				Value: "error",
			},
		},
		Commands: local,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
}

var addCmd = &cli.Command{
	Name:  "add",
	Usage: "add key/value",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "target",
			Usage:    "",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "n",
			Usage: "",
			Value: 2,
		},
		&cli.StringFlag{
			Name:  "protocol",
			Usage: "",
			Value: "tcp",
		},
	},
	Action: func(c *cli.Context) error {
		logging.SetLogLevel("*", "info")
		ctx := context.Background()

		client := trans.NewTransClient(ctx, c.String("target"), c.Int("n"), c.String("protocol"))
		defer client.Close()

		args := c.Args().Slice()
		if len(args) != 2 {
			return fmt.Errorf("[usage]: pshelper cat [key] [path]")
		}
		v, err := ioutil.ReadFile(args[1])
		if err != nil {
			return err
		}
		return client.Put(args[0], v)

		// f, err := os.Open(args[1])
		// if err != nil {
		// 	return err
		// }
		// defer f.Close()
		// buf := make([]byte, 409600)
		// count := 0
		// for {
		// 	isEnd := false
		// 	n, err := io.ReadFull(f, buf)
		// 	if err != nil {
		// 		if err == io.EOF || err == io.ErrUnexpectedEOF {
		// 			isEnd = true
		// 		} else {
		// 			break
		// 		}
		// 	}
		// 	if err := client.Put(fmt.Sprintf("%s_%d", args[0], count), buf[:n]); err != nil {
		// 		logger.Error(err)
		// 		break
		// 	}
		// 	if isEnd {
		// 		break
		// 	}
		// 	count++
		// }
		// return nil
	},
}

var catCmd = &cli.Command{
	Name:  "cat",
	Usage: "print value for key",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "target",
			Usage:    "",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "n",
			Usage: "",
			Value: 2,
		},
		&cli.StringFlag{
			Name:  "protocol",
			Usage: "",
			Value: "tcp",
		},
	},
	Action: func(c *cli.Context) error {
		logging.SetLogLevel("*", "info")
		ctx := context.Background()

		client := trans.NewTransClient(ctx, c.String("target"), c.Int("n"), c.String("protocol"))

		key := c.Args().First()
		if key == "" {
			return fmt.Errorf("[usage]: pshelper cat [key]")
		}
		bs, err := client.Get(key)
		if err != nil {
			return err
		}
		fmt.Println(bs)
		return nil
	},
}
