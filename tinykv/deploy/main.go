package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	scheduler = "tinyscheduler-server"
	tinykv    = "tinykv-server"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cluster subcommand",
		Short: "simple cluster operation tool",
	}

	nodeNumber    int
	deployPath    string
	binaryPath    string
	schedulerPort = 2379
	kvPort        = 20160

	deployCmd = &cobra.Command{
		Use:   "deploy",
		Short: "deploy a local cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Build the new binaries.
			_, err := exec.Command("make", "scheduler").Output()
			if err != nil {
				log.Fatal("error happened build scheduler calling make scheduler", zap.Error(err))
			}
			_, err = exec.Command("make", "kv").Output()
			if err != nil {
				log.Fatal("error happened when build tinykv calling make kv", zap.Error(err))
			}

			// Create the deploy path.
			if _, err := os.Stat(deployPath); os.IsNotExist(err) {
				err := os.Mkdir(deployPath, os.ModePerm)
				if err != nil {
					log.Fatal("create deploy dir failed", zap.Error(err))
				}
			} else {
				log.Fatal("the dir already exists, please remove it first", zap.String("deploy path", deployPath))
			}

			// Create the scheduler and kv path, and copy new binaries to the target path.
			schedulerPath := path.Join(deployPath, "scheduler")
			err = os.Mkdir(schedulerPath, os.ModePerm)
			if err != nil {
				log.Fatal("create scheduler dir failed", zap.Error(err))
			}
			_, err = exec.Command("cp", path.Join(binaryPath, scheduler), schedulerPath).Output()
			if err != nil {
				log.Fatal("copy scheduler binary to path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("schedulerPath", schedulerPath))
			}
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				err = os.Mkdir(kvPath, os.ModePerm)
				if err != nil {
					log.Fatal("create tinykv dir failed", zap.Error(err))
				}
				_, err = exec.Command("cp", path.Join(binaryPath, tinykv), kvPath).Output()
				if err != nil {
					log.Fatal("copy tinykv binary to path failed", zap.Error(err),
						zap.String("binaryPath", binaryPath), zap.String("tinykvPath", kvPath))
				}
			}
		},
	}

	startCmd = &cobra.Command{
		Use:   "start",
		Short: "start the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Try to start the scheduler server.
			log.Info("start info", zap.Int("scheduler port", schedulerPort), zap.Int("kv port", kvPort))
			_, err := Check(schedulerPort)
			if err != nil {
				log.Fatal("check scheduler port failed", zap.Error(err), zap.Int("port", schedulerPort))
			}
			schedulerPath := path.Join(deployPath, "scheduler")
			t := time.Now()
			tstr := t.Format("20060102150405")
			logName := fmt.Sprintf("log_%s", tstr)
			startSchedulerCmd := fmt.Sprintf("nohup ./%s > %s 2>&1 &", scheduler, logName)
			log.Info("start scheduler cmd", zap.String("cmd", startSchedulerCmd))
			shellCmd := exec.Command("bash", "-c", startSchedulerCmd)
			shellCmd.Dir = schedulerPath
			_, err = shellCmd.Output()
			if err != nil {
				os.Remove(path.Join(schedulerPath, logName))
				log.Fatal("start scheduler failed", zap.Error(err))
			}
			err = waitPortUse([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port in use error", zap.Error(err))
			}

			// Try to start the tinykv server.
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				thisKVPort := kvPort + i
				_, err := Check(thisKVPort)
				if err != nil {
					log.Fatal("check kv port failed", zap.Error(err), zap.Int("kv port", thisKVPort))
				}
				startKvCmd := fmt.Sprintf(`nohup ./%s > %s --path . --addr "127.0.0.1:%d" 2>&1 &`, tinykv, logName, thisKVPort)
				log.Info("start tinykv cmd", zap.String("cmd", startKvCmd))
				shellCmd := exec.Command("bash", "-c", startKvCmd)
				shellCmd.Dir = kvPath
				_, err = shellCmd.Output()
				if err != nil {
					os.Remove(path.Join(kvPath, logName))
					log.Fatal("start scheduler failed", zap.Error(err))
				}
				ports = append(ports, thisKVPort)
			}
			err = waitPortUse(ports)
			if err != nil {
				log.Fatal("wait tinykv port in use error", zap.Error(err))
			}

			log.Info("start cluster finished")
		},
	}

	stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "stop the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Try to stop the tinykv server.
			killScheduler := fmt.Sprintf("pkill -f %s", scheduler)
			log.Info("start scheduler cmd", zap.String("cmd", killScheduler))
			shellCmd := exec.Command("bash", "-c", killScheduler)
			_, err := shellCmd.Output()
			if err != nil {
				log.Fatal("stop scheduler failed", zap.Error(err))
			}
			err = waitPortFree([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port free error", zap.Error(err))
			}

			// Try to stop the scheduler server.
			killKvServer := fmt.Sprintf("pkill -f %s", tinykv)
			log.Info("start scheduler cmd", zap.String("cmd", killKvServer))
			shellCmd = exec.Command("bash", "-c", killKvServer)
			_, err = shellCmd.Output()
			if err != nil {
				log.Fatal("stop tinykv failed", zap.Error(err))
			}
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				thisKVPort := kvPort + i
				ports = append(ports, thisKVPort)
			}
			err = waitPortFree(ports)
			if err != nil {
				log.Fatal("wait tinykv port free error", zap.Error(err))
			}
		},
	}

	upgradeCmd = &cobra.Command{
		Use:   "upgrade",
		Short: "upgrade the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Check the cluster is stopped.
			err := waitPortFree([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port free error", zap.Error(err))
			}
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				thisKVPort := kvPort + i
				ports = append(ports, thisKVPort)
			}
			err = waitPortFree(ports)
			if err != nil {
				log.Fatal("wait tinykv port free error", zap.Error(err))
			}

			// Rebuild the binary.
			_, err = exec.Command("make", "scheduler").Output()
			if err != nil {
				log.Fatal("error happened build scheduler calling make scheduler", zap.Error(err))
			}
			_, err = exec.Command("make", "kv").Output()
			if err != nil {
				log.Fatal("error happened when build tinykv calling make kv", zap.Error(err))
			}

			// Substitute the binary.
			schedulerPath := path.Join(deployPath, "scheduler")
			_, err = exec.Command("cp", path.Join(binaryPath, scheduler), schedulerPath).Output()
			if err != nil {
				log.Fatal("copy scheduler binary to path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("schedulerPath", schedulerPath))
			}
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				_, err = exec.Command("cp", path.Join(binaryPath, tinykv), kvPath).Output()
				if err != nil {
					log.Fatal("copy tinykv binary to path failed", zap.Error(err),
						zap.String("binaryPath", binaryPath), zap.String("kvPath", kvPath))
				}
			}
		},
	}

	destroyCmd = &cobra.Command{
		Use:   "destroy",
		Short: "destroy the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("destroy is starting to remove the whole deployed directory",
				zap.String("deployPath", deployPath))
			err := os.RemoveAll(deployPath)
			if err != nil {
				log.Fatal("remove the deploy path failed", zap.Error(err))
			}
			log.Info("cleanup finished")
		},
	}
)

// Check if a port is available
func Check(port int) (status bool, err error) {

	// Concatenate a colon and the port
	host := ":" + strconv.Itoa(port)

	// Try to create a server with the port
	server, err := net.Listen("tcp", host)

	// if it fails then the port is likely taken
	if err != nil {
		return false, err
	}

	// close the server
	server.Close()

	// we successfully used and closed the port
	// so it's now available to be used again
	return true, nil

}

const waitDurationLimit = time.Duration(2 * time.Minute)
const waitSleepDuration = time.Duration(300 * time.Millisecond)

func checkLoop(waitPorts []int, checkFn func([]int) bool) error {
	waitStart := time.Now()
	for {
		if time.Since(waitStart) > waitDurationLimit {
			log.Error("check port free timeout")
			return errors.New("check port free timeout")
		}
		if checkFn(waitPorts) {
			break
		}
		time.Sleep(waitSleepDuration)
	}
	return nil
}

func waitPortFree(waitPorts []int) error {
	return checkLoop(waitPorts, func(ports []int) bool {
		allFree := true
		for _, port := range waitPorts {
			_, err := Check(port)
			if err != nil {
				allFree = false
				break
			}
		}
		return allFree
	})
}

func waitPortUse(waitPorts []int) error {
	return checkLoop(waitPorts, func(ports []int) bool {
		allInUse := true
		for _, port := range waitPorts {
			_, err := Check(port)
			if err == nil {
				allInUse = false
				break
			}
		}
		return allInUse
	})
}

func init() {
	rootCmd.Flags().IntVarP(&nodeNumber, "num", "n", 3, "the number of the tinykv servers")
	rootCmd.Flags().StringVarP(&deployPath, "deploy_path", "d", "./bin/deploy", "the deploy path")
	rootCmd.Flags().StringVarP(&binaryPath, "binary_path", "b", "./bin", "the binary path")

	rootCmd.AddCommand(deployCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(stopCmd)
	rootCmd.AddCommand(upgradeCmd)
	rootCmd.AddCommand(destroyCmd)
}

func main() {
	rootCmd.Execute()
}
