// Package main 为Docker容器实现一个轻量级systemctl替代方案。
// 它提供基本的systemd服务管理功能，无需完整的systemd安装。
package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/unit"
)

// 遵循systemd约定的全局配置路径
var (
	// sysPath 是系统级的systemd服务文件目录
	sysPath = "/usr/lib/systemd/system"
	// usrPath 是本地systemd服务文件目录
	usrPath = "/etc/systemd/system"
	// enablePath 是已启用服务的目录（符号链接）
	enablePath = "/etc/systemd/system/multi-user.target.wants"
	// socketPath 是守护进程通信的Unix套接字路径
	socketPath = "/etc/systemd/systemctl.sock"
	// mapCommand 跟踪正在运行的服务及其进程
	mapCommand = map[string]*exec.Cmd{}
	// lock 保护对mapCommand的并发访问
	lock sync.Mutex
)

// reapZombies 持续回收僵尸进程以防止资源泄露。
// 此函数在goroutine中运行，检查已终止的子进程。
func reapZombies() {
	for {
		var status syscall.WaitStatus
		// WNOHANG: 非阻塞模式，如果没有僵尸进程立即返回
		// -1: 等待任何子进程
		pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
		if err != nil || pid <= 0 {
			// 没有僵尸进程或发生错误
			time.Sleep(1 * time.Second)
			continue
		}
		log.Printf("Reaped zombie process PID: %d\n", pid)
	}
}

// main 是systemctl程序的入口点。
// 它处理命令行参数并将它们路由到适当的函数。
func main() {
	args := os.Args

	// 当以"reboot"调用时处理重启命令
	if strings.Contains(os.Args[0], "reboot") {
		send("reboot", "reboot")
		return
	}

	// 至少需要一个参数
	if len(args) < 2 {
		fmt.Println("Usage: systemctl [enable|disable|start|stop|restart|status|domain] [service]")
		return
	}

	// 将命令路由到适当的处理程序
	switch args[1] {
	case "enable":
		if len(args) < 3 {
			fmt.Println("Error: service name required")
			return
		}
		log.Printf("Enabling service: %s\n", args[2])
		fmt.Println(send(args[2], "enable"))
	case "disable":
		if len(args) < 3 {
			fmt.Println("Error: service name required")
			return
		}
		log.Printf("Disabling service: %s\n", args[2])
		fmt.Println(send(args[2], "disable"))
	case "start":
		if len(args) < 3 {
			fmt.Println("Error: service name required")
			return
		}
		log.Printf("Starting service: %s\n", args[2])
		fmt.Println(send(args[2], "start"))
	case "stop":
		if len(args) < 3 {
			fmt.Println("Error: service name required")
			return
		}
		log.Printf("Stopping service: %s\n", args[2])
		fmt.Println(send(args[2], "stop"))
	case "restart":
		if len(args) < 3 {
			fmt.Println("Error: service name required")
			return
		}
		log.Printf("Restarting service: %s\n", args[2])
		fmt.Println(send(args[2], "restart"))
	case "status":
		if len(args) < 3 {
			fmt.Println("Error: service name required")
			return
		}
		log.Printf("Checking service status: %s\n", args[2])
		fmt.Println(send(args[2], "status"))
	case "domain":
		log.Println("Starting daemon process")
		// 启动僵尸进程回收器
		go reapZombies()
		Domain()
	case "--version":
		fmt.Println("systemd 226")
	default:
		fmt.Printf("Unknown command: %s\n", args[1])
		fmt.Println("Usage: systemctl [enable|disable|start|stop|restart|status|domain] [service]")
	}
}

// parseSystemdService 解析systemd服务文件内容并返回单元选项。
func parseSystemdService(content string) ([]*unit.UnitOption, error) {
	opts, err := unit.Deserialize(strings.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("failed to parse service file: %w", err)
	}
	return opts, nil
}

// getOptions 从解析的systemd单元选项中检索特定选项值。
func getOptions(list []*unit.UnitOption, section string, name string) (string, error) {
	for _, option := range list {
		if option.Section == section && option.Name == name {
			return option.Value, nil
		}
	}
	return "", fmt.Errorf("option %s.%s not found", section, name)
}

// send 通过Unix套接字与守护进程通信。
// 它发送命令和服务名称，然后返回守护进程的响应。
func send(service, op string) string {
	// 连接到Unix域套接字
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return fmt.Sprintf("Error connecting to daemon: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// 以"operation:service"格式发送消息
	msg := fmt.Sprintf("%s:%s", op, service)
	_, err = conn.Write([]byte(msg))
	if err != nil {
		return fmt.Sprintf("Error sending message: %v", err)
	}

	// 接收守护进程的响应
	var response [1024]byte
	n, err := conn.Read(response[:])
	if err != nil {
		return fmt.Sprintf("Error reading response: %v", err)
	}
	return string(response[:n])
}

// find 通过在标准systemd目录中搜索来定位服务文件。
// 它首先检查用户路径，然后回退到系统路径。
func find(service string) string {
	// 首先检查用户定义的服务目录
	path := fmt.Sprintf("%s/%s.service", usrPath, service)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// 回退到系统服务目录
	path = fmt.Sprintf("%s/%s.service", sysPath, service)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// 未找到服务文件
	return ""
}

// Domain 启动管理systemd服务的守护进程。
// 它自动启动已启用的服务并通过Unix套接字监听客户端命令。
func Domain() {
	err := filepath.Walk(enablePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".service") {
			if info.Name() == "e2scrub_reap.service" {
				return nil
			}
			err = Start(strings.TrimSuffix(info.Name(), ".service"), 5)
			if err != nil {
				log.Printf("Failed to auto-start service %s: %v\n", info.Name(), err)
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("find server err: %v\n", err)
		return
	}
	// 如果文件已存在，先删除
	if _, err = os.Stat(socketPath); err == nil {
		if err = os.Remove(socketPath); err != nil {
			log.Println("err:", err)
			return
		}
	}
	// 创建Unix域套接字监听器
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Println("Listening failed:", err)
	}
	defer func() { _ = listener.Close() }()

	// 设置文件权限
	if err = os.Chmod(socketPath, 0777); err != nil {
		log.Println("Failed to set file permissions:", err)
	}

	// 处理中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = listener.Close()
		_ = os.Remove(socketPath)
		os.Exit(0)
	}()

	for {
		// 接受连接
		conn, err2 := listener.Accept()
		if err2 != nil {
			return
		}
		// 处理连接
		go handleConnection(conn)
	}
}

// handleConnection 处理到守护进程的单个客户端连接。
// 它解析传入的命令并将其分派到适当的处理程序。
func handleConnection(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}
	msg := string(buf[:n])
	split := strings.Split(msg, ":")
	if len(split) < 2 {
		return
	}
	split[1] = strings.ReplaceAll(split[1], ".service", "")
	switch split[0] {
	case "enable":
		log.Println("enable:", split[1])
		err = Enable(split[1])
	case "disable":
		log.Println("disable:", split[1])
		err = Disable(split[1])
	case "start":
		log.Println("start:", split[1])
		err = Start(split[1], 5)
	case "stop":
		log.Println("stop:", split[1])
		err = Stop(split[1])
	case "status":
		log.Println("status:", split[1])
		res, err2 := Status(split[1])
		if err2 != nil {
			err = err2
		} else {
			_, _ = conn.Write([]byte(res))
			return
		}
	case "reboot":
		log.Println("reboot")
		os.Exit(0)
	}
	if err != nil {
		_, _ = conn.Write([]byte(err.Error()))
		return
	} else {
		_, _ = conn.Write([]byte("success"))
		return
	}
}

// Enable 在multi-user.target.wants目录中为服务创建符号链接。
// 这使得服务在守护进程启动时自动启动。
func Enable(service string) error {
	lock.Lock()
	defer lock.Unlock()
	path := find(service)
	if path == "" {
		return errors.New("no service found")
	}
	err := os.Symlink(path, fmt.Sprintf("%s/%s.service", enablePath, service))
	if err != nil {
		return err
	}
	return nil
}

// Disable 从multi-user.target.wants目录中移除服务的符号链接。
// 这防止服务自动启动。
func Disable(service string) error {
	lock.Lock()
	defer lock.Unlock()
	err := os.Remove(fmt.Sprintf("%s/%s.service", enablePath, service))
	if err != nil {
		return err
	}
	return nil
}

// Start 基于systemd服务文件启动服务进程。
// 它支持重启策略和失败时的自动重试。
func Start(service string, try int) error {
	lock.Lock()
	defer lock.Unlock()

	log.Printf("Starting service: %s (attempts: %d)\n", service, try)

	path := find(service)
	if path == "" {
		log.Printf("Service file not found: %s\n", service)
		return errors.New("no service found")
	}

	file, err := os.ReadFile(path)
	if err != nil {
		log.Printf("Failed to read service file: %v\n", err)
		return err
	}

	systemdService, err := parseSystemdService(string(file))
	if err != nil {
		log.Printf("Failed to parse service file: %v\n", err)
		return err
	}

	process := mapCommand[service]
	if process != nil && process.Process != nil {
		log.Printf("Terminating existing service process: %s\n", service)
		_ = process.Process.Signal(syscall.SIGTERM)
		_ = process.Wait()
	}

	val, err := getOptions(systemdService, "Service", "ExecStart")
	if err != nil {
		log.Printf("Failed to get ExecStart: %v\n", err)
		return err
	}
	if val == "" {
		log.Printf("ExecStart not configured: %s\n", service)
		return errors.New("ExecStart not found")
	}

	val2, _ := getOptions(systemdService, "Service", "WorkingDirectory")
	// 解析命令
	split := strings.Split(val, " ")
	var cmdArgs []string
	if len(split) > 1 {
		for _, s := range split[1:] {
			if strings.HasPrefix(s, "$") {
				getenv := os.Getenv(s)
				if getenv != "" {
					cmdArgs = append(cmdArgs, getenv)
				}
			} else {
				cmdArgs = append(cmdArgs, s)
			}
		}
	}
	command := exec.Command(split[0], cmdArgs...)
	// 设置工作目录
	if val2 != "" {
		command.Dir = val2
	} else {
		command.Dir = "/root"
	}
	mapCommand[service] = command
	log.Printf("Executing command: %s\n", command.String())
	command.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}
	err = command.Start()
	if err != nil {
		log.Printf("Failed to start service: %v\n", err)
		return err
	}

	log.Printf("Service started successfully: %s (PID: %d)\n", service, command.Process.Pid)

	go func() {
		_ = command.Wait()
		exitCode := command.ProcessState.ExitCode()
		log.Printf("Service exited: %s (exit code: %d)\n", service, exitCode)

		val, _ = getOptions(systemdService, "Service", "Restart")
		if val == "always" && mapCommand[service] == nil {
			log.Printf("Service %s configured for always restart, but service has been removed\n", service)
			return
		}
		if val == "on-failure" && exitCode == 0 {
			log.Printf("Service %s exited normally, no restart needed\n", service)
			return
		}

		time.Sleep(time.Second * 5)
		if try > 0 {
			log.Printf("Attempting to restart service: %s (remaining attempts: %d)\n", service, try-1)
			err = Start(service, try-1)
			if err != nil {
				log.Printf("Failed to restart service: %v\n", err)
			}
		}
	}()

	return nil
}

// Stop 优雅地终止正在运行的服务进程。
// 它首先发送SIGTERM，如果进程在5秒内没有退出则发送SIGKILL。
func Stop(service string) error {
	lock.Lock()
	defer lock.Unlock()

	command := mapCommand[service]
	if command == nil {
		return errors.New("service is not run")
	}
	// 1. 尝试正常终止（SIGTERM）
	err := command.Process.Signal(syscall.SIGTERM)
	if err != nil {
		log.Printf("Failed to send SIGTERM: %v\n", err)
	}

	// 2. 等待进程退出（最多 5 秒）
	done := make(chan error, 1)
	go func() {
		_, err = command.Process.Wait() // 回收子进程，避免僵尸进程
		done <- err
	}()

	select {
	case <-time.After(5 * time.Second):
		// 3. 超时后强制终止（SIGKILL）
		log.Println("The process did not exit normally, forcing termination...")
		err = command.Process.Signal(syscall.SIGKILL)
		if err != nil {
			log.Printf("Failed to send SIGKILL: %v\n", err)
		}
	case err = <-done:
		if err != nil {
			log.Printf("Failed waiting for process exit: %v\n", err)
		}
	}
	// 4. 从 map 中移除 PID
	delete(mapCommand, service)
	return nil
}

// Status 检查服务是否正在运行。
// 根据进程状态返回"running"或"exited"。
func Status(service string) (string, error) {
	lock.Lock()
	defer lock.Unlock()
	path := find(service)
	if path == "" {
		return "", errors.New("no service found")
	}
	command := mapCommand[service]
	if command == nil || command.Process == nil || !isProcessRunning(command.Process.Pid) {
		return "exited", nil
	}
	return "running", nil
}

// isProcessRunning 检查给定PID的进程是否仍然存活。
// 它使用信号0测试进程存在性而不实际发送信号。
func isProcessRunning(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil
}
