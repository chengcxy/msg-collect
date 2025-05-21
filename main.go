package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	maxFileSize = 128 * 1024 * 1024 // 128MB
	bulkSize    = 100               // 每批写入记录数
)

// FileWriter 用于写 JSON 文件，并在超出大小时切换
type FileWriter struct {
	baseName    string
	file        *os.File
	currentSize int64
	counter     int64
}

// NewFileWriter 初始化文件写入器
func NewFileWriter(base string) *FileWriter {
	return &FileWriter{
		baseName: base,
		counter:  0,
	}
}

// rotate 创建新文件
func (fw *FileWriter) rotate() error {
	if fw.file != nil {
		fw.file.Close()
	}
	timestamp := time.Now().Format("20060102150405")
	filename := fmt.Sprintf("%s_%s_%06d.json", fw.baseName, timestamp, fw.counter)
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create file failed: %w", err)
	}
	fw.file = file
	fw.currentSize = 0
	fw.counter++
	fmt.Printf("[INFO] Created new file: %s\n", filename)
	return nil
}

// Write 写入数据并判断是否超出大小
func (fw *FileWriter) Write(data []byte) error {
	if fw.file == nil || fw.currentSize+int64(len(data)) > maxFileSize {
		if err := fw.rotate(); err != nil {
			return err
		}
	}
	n, err := fw.file.Write(data)
	if err != nil {
		return fmt.Errorf("write file failed: %w", err)
	}
	fw.currentSize += int64(n)
	return nil
}

// Close 关闭文件
func (fw *FileWriter) Close() {
	if fw.file != nil {
		fw.file.Close()
	}
}

// Produce 模拟生成数据
func Produce(ctx context.Context, messageChan chan map[string]interface{}) {
	defer func(){
		close(messageChan)
	}()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("[INFO] Producer received cancel signal.")
			return
		default:
			msg := map[string]interface{}{
				"id": rand.Intn(100),
				"ts": time.Now().UnixNano(),
			}
			messageChan <- msg
		}
	}
}

// doBulk 写入一批数据到文件
func doBulk(fw *FileWriter, reqs []map[string]interface{}) error {
	if len(reqs) == 0 {
		return nil
	}
	lines := make([]string, len(reqs))
	for i, req := range reqs {
		line, _ := json.Marshal(req)
		lines[i] = string(line)
	}
	data := strings.Join(lines, "\n") + "\n"
	if err := fw.Write([]byte(data)); err != nil {
		return fmt.Errorf("file write error: %w", err)
	}
	fmt.Printf("[INFO] Wrote %d records (%.2f KB)\n", len(reqs), float64(len(data))/1024)
	return nil
}

// consumer 消费者逻辑：定期或按量落盘
func consumer(ctx context.Context, messageChan chan map[string]interface{}, done chan struct{}) {
	ticker := time.NewTicker(180 * time.Second)
	defer ticker.Stop()
	defer func() { 
		done <- struct{}{} 
	}()

	reqs := make([]map[string]interface{}, 0, 1024)
	fw := NewFileWriter("binlog_data")
	defer fw.Close()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("[INFO] Consumer received cancel signal.")
			if len(reqs) > 0 {
				_ = doBulk(fw, reqs)
			}
			return
		case v, ok := <-messageChan:
			if !ok {
				if len(reqs) > 0 {
					_ = doBulk(fw, reqs)
				}
				return
			}
			reqs = append(reqs, v)
			if len(reqs) >= bulkSize {
				if err := doBulk(fw, reqs); err != nil {
					fmt.Printf("[ERROR] Bulk write failed: %v\n", err)
				}
				reqs = reqs[:0]
			}
		case <-ticker.C:
			if len(reqs) > 0 {
				if err := doBulk(fw, reqs); err != nil {
					fmt.Printf("[ERROR] Timed bulk write failed: %v\n", err)
				}
				reqs = reqs[:0]
			}
		}
	}
}

func main() {
	messageChan := make(chan map[string]interface{}, 1000)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	// 监听信号
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go Produce(ctx, messageChan)
	go consumer(ctx, messageChan, done)
	go func() {
		<-sigs
		fmt.Println("[INFO] Received kill signal, exiting...")
		cancel()
	}()

	<-done
	fmt.Println("[INFO] Program exited cleanly.")
}
