package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	kv "github.com/filedag-project/mutcask"
	"github.com/filedag-project/trans"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var logger = logging.Logger("serv")

func main() {
	local := []*cli.Command{
		daemonCmd,
	}

	app := &cli.App{
		Name: "serv",
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

var daemonCmd = &cli.Command{
	Name:  "daemon",
	Usage: "",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "listen",
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
		&cli.StringFlag{
			Name:     "repo",
			Usage:    "",
			Required: true,
		},
	},
	Action: func(c *cli.Context) error {
		logging.SetLogLevel("*", "info")
		ctx := context.Background()

		db, err := kv.NewMutcask(kv.PathConf(c.String("repo")), kv.CaskNumConf(4))
		if err != nil {
			panic(err)
		}

		pserv, err := trans.NewPServ(ctx, c.String("listen"), db, c.String("protocol"))
		if err != nil {
			panic(err)
		}
		defer pserv.Close()

		quit := make(chan os.Signal, 1)

		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		return nil
	},
}
