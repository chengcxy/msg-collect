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
	dbs := []string{"db1", "db2"}
	tables := []string{"table1", "table2"}
	for {
		select {
		case <-ctx.Done():
			fmt.Println("[INFO] Producer received cancel signal.")
			return
		default:
			msg := map[string]interface{}{
				"id":         rand.Intn(1000),
				"ts":         time.Now().UnixNano(),
				"db":         dbs[rand.Intn(len(dbs))],
				"table_name": tables[rand.Intn(len(tables))],
			}
			messageChan <- msg
		}
	}
}

// doBulk 将数据按 db.table 分组写入对应文件
func doBulk(writers map[string]*FileWriter, reqs []map[string]interface{}) error {
	grouped := make(map[string][]string)

	// 分组
	for _, req := range reqs {
		db, _ := req["db"].(string)
		table, _ := req["table_name"].(string)
		if db == "" || table == "" {
			continue
		}
		key := fmt.Sprintf("%s.%s", db, table)
		jsonLine, _ := json.Marshal(req)
		grouped[key] = append(grouped[key], string(jsonLine))
	}

	// 写入
	for key, lines := range grouped {
		data := strings.Join(lines, "\n") + "\n"
		safeKey := strings.ReplaceAll(key, ".", "_") // 文件名安全
		fw, ok := writers[key]
		if !ok {
			fw = NewFileWriter(safeKey)
			writers[key] = fw
		}
		if err := fw.Write([]byte(data)); err != nil {
			fmt.Printf("[ERROR] Write failed for %s: %v\n", key, err)
		} else {
			fmt.Printf("[INFO] Wrote %d records to %s\n", len(lines), key)
		}
	}
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
	writers := make(map[string]*FileWriter)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("[INFO] Consumer received cancel signal.")
			if len(reqs) > 0 {
				_ = doBulk(writers, reqs)
			}
			for _, w := range writers {
				w.Close()
			}
			return
		case v, ok := <-messageChan:
			if !ok {
				if len(reqs) > 0 {
					_ = doBulk(writers, reqs)
				}
				for _, w := range writers {
					w.Close()
				}
				return
			}
			reqs = append(reqs, v)
			if len(reqs) >= bulkSize {
				_ = doBulk(writers, reqs)
				reqs = reqs[:0]
			}
		case <-ticker.C:
			if len(reqs) > 0 {
				_ = doBulk(writers, reqs)
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
