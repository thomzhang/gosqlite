package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unsafe"
	// "io/ioutil"
)
// Row 数据的每一行
type Row struct {
	ID int32   // 8
	UserName string  // 20
	Email string     // 40
}


const (
	PageSize = 4096    // 一页最大的内存
	TABLE_MAX_PAGES  = 100
	ID_SIZE = 8
	USERNAME_SIZE = 20
	EMAIL_SIZE = 40
	ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
	ROWS_PER_PAGE = PageSize/ROW_SIZE
)

var dbfileName = flag.String("dbfilename", "db.txt", "Input Your DB File Name")


func printPrompt() {
	fmt.Printf("db> ")
}

func readInput() (string, error){
	fmt.Scan()
	input, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", err
	}
	input = strings.Replace(input, "\n", "", -1)
	return input, nil
}

type  metaCommandType uint32
// todo 枚举类型自增
const (
	metaCommandSuccess      metaCommandType = 0
	metaCommandUnRecongnizedCommand metaCommandType = 1
)

type StatementType int32
const (
	statementUnknown StatementType = 0
	statementInsert StatementType = 1
	statementSelect StatementType = 2
)

type executeResult int32
const  (
	ExecuteSuccess executeResult = 0
	ExecuteTableFull executeResult = 1
	EXECUTE_DUPLICATE_KEY executeResult = 2
)

type PrepareType int32
const  (
	prepareSuccess PrepareType = 0
	prepareUnrecognizedStatement PrepareType = 1
	prepareUnrecognizedSynaErr PrepareType = 2
)

type Statement struct {
	statementType StatementType;
	rowToInsert Row;
}

// do_meta_command
func doMetaCommand(input string, table *Table) metaCommandType {
	if input == ".exit" {
		dbClose(table)
		os.Exit(0)
		return metaCommandSuccess
	}
	if input == ".btree" {
		fmt.Printf("Tree:\n");
		print_leaf_node(getPage(table.pager, 0));
		return metaCommandSuccess;
	}
	if input == ".constants" {
		fmt.Printf("Constants:\n");
		print_constants();
		return metaCommandSuccess
	}
	return metaCommandUnRecongnizedCommand
}

func prepareStatement(input string, statement *Statement)PrepareType {
	if  len(input) >= 6 && input[0:6] == "insert" {
		statement.statementType = statementInsert
		inputs := strings.Split(input, " ")
		if len(inputs) <=1 {
			return prepareUnrecognizedStatement
		}
		id, err := strconv.ParseInt(inputs[1], 10, 64)
		if err != nil {
			return prepareUnrecognizedSynaErr
		}
		statement.rowToInsert.ID =  int32(id)
		statement.rowToInsert.UserName = inputs[2]
		statement.rowToInsert.Email = inputs[3]
		return prepareSuccess
	}
	if len(input) >= 6 && input[0:6] == "select" {
		statement.statementType = statementSelect
		return prepareSuccess
	}
	return prepareUnrecognizedStatement
}

// Pager 管理数据从磁盘到内存
type Pager struct {
	osfile     *os.File;
	fileLength int64;
	numPages   uint32;
	pages      []unsafe.Pointer;  // 存储数据
}

// Table 数据库表
type Table struct {
	rootPageNum uint32;
	pager       *Pager;
}

func printRow(row *Row)  {
	fmt.Println(row.ID, row.UserName, row.Email)
}
func Uint32ToBytes(n int32) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func serializeRow(row *Row, destionaton unsafe.Pointer) {
	ids := Uint32ToBytes(row.ID)
	q := (*[ROW_SIZE]byte)(destionaton)
	copy(q[0:ID_SIZE], ids)
	copy(q[ID_SIZE+1:ID_SIZE+USERNAME_SIZE], (row.UserName))
	copy(q[ID_SIZE+USERNAME_SIZE+1: ROW_SIZE], (row.Email))
}
//字节转换成整形
func BytesToInt32(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

func deserializeRow(source unsafe.Pointer, rowDestination *Row) {
	ids := make([]byte, ID_SIZE, ID_SIZE)
	sourceByte := (*[ROW_SIZE]byte)(source)
	copy(ids[0:ID_SIZE], (*sourceByte)[0:ID_SIZE])
	rowDestination.ID = BytesToInt32(ids)
	userName := make([]byte, USERNAME_SIZE, USERNAME_SIZE)
	copy(userName[0:], (*sourceByte)[ID_SIZE+1: ID_SIZE + USERNAME_SIZE])
	// todo 先用死办法去掉反斜杆
	realNameBytes := getUseFulByte(userName)
	rowDestination.UserName = (string)(realNameBytes)
	emailStoreByte :=  make([]byte, EMAIL_SIZE, EMAIL_SIZE)
	copy(emailStoreByte[0:], (*sourceByte)[1+ ID_SIZE + USERNAME_SIZE: ROW_SIZE])
	emailByte := getUseFulByte(emailStoreByte)
	rowDestination.Email = (string)(emailByte)
}

// getUseFulByte 去掉没有赋值的byte，并返回出去
func getUseFulByte(curBytes []byte) []byte {
	realBytes := make([]byte, 0, 1)
	var notExitByte byte
	for _, v := range curBytes {
		if v == notExitByte {
			break
		}
		realBytes = append(realBytes, v)
	}
	return realBytes
}

// 获取一页的数据，当缓存消失的时候，我们应该从文件里面加载到缓存里面
func getPage(pager *Pager, pageNum uint32)  unsafe.Pointer   {
	if pageNum > TABLE_MAX_PAGES {
		fmt.Println("Tried to fetch page number out of bounds:", pageNum)
		os.Exit(0)
	}
	if pager.pages[pageNum] == nil {
		page := make([]byte, PageSize)
		numPage := uint32(pager.fileLength/PageSize)  // 第几页
		if pager.fileLength%PageSize == 0 {
			numPage += 1
		}
		if pageNum <= numPage {
			curOffset := pageNum*PageSize
			// 偏移到下次可以读读未知
			curNum, err := pager.osfile.Seek(int64(curOffset), io.SeekStart)
			if err != nil {
				panic(err)
			}
			fmt.Println(curNum)
			// 读到偏移这一页到下一页，必须是真的有多少字符
			if _,err = pager.osfile.ReadAt(page, curNum);err != nil && err != io.EOF{
				panic(err)
			}
		}
		pager.pages[pageNum] = unsafe.Pointer(&page[0])
		if pageNum >= pager.numPages {
			pager.numPages = pageNum +1
		}
	}
	return pager.pages[pageNum]
}

func executeInsert(statement *Statement, table *Table) executeResult{
	node := getPage(table.pager, table.rootPageNum)
	numCells := (*leaf_node_num_cells(node));

	if numCells >= LEAF_NODE_MAX_CELLS {
		return ExecuteTableFull;
	}
	row := &Row{
		ID: statement.rowToInsert.ID,
		UserName: statement.rowToInsert.UserName,
		Email: statement.rowToInsert.Email,
	}
	// 找到游标
	cursor := tableFind(table, uint32(row.ID));
	if (cursor.cellNum < numCells) {
		key_at_index := *leaf_node_key(node, cursor.cellNum)
		if uint32(row.ID) == key_at_index {
			return EXECUTE_DUPLICATE_KEY;
		}
	}
	// 往游标的左叶子节点插入
	leaf_node_insert(cursor, uint32(row.ID), row)
	return ExecuteSuccess
}

func executeSelect(statement *Statement, table *Table) executeResult{
	cursor := tableStart(table)
	for ; !cursor.endOfTable; {
		row := Row{}
		deserializeRow(cursorValue(cursor), &row)
		printRow(&row)
		cursorAdvance(cursor)
	}

	//var i uint32
	//for ; i < table.rowNum; i++ {
	//	row := &Row{}
	//	deserializeRow(rowSLot(table, i), row)
	//	if row.ID == 0 {
	//		continue
	//	}
	//	printRow(row)
	//}
	return ExecuteSuccess
}

func dbClose(table *Table) {
	for i:= uint32(0); i < table.pager.numPages; i++ {
		if table.pager.pages[i] == nil {
			continue
		}
		pagerFlush(table.pager, i, PageSize);
	}
	defer table.pager.osfile.Close()
	// go语言自带gc
}

// pagerFlush 这一页写入文件系统
func pagerFlush(pager *Pager, pageNum , realNum uint32) error{
	if pager.pages[pageNum] == nil {
		return fmt.Errorf("pagerFlush null page")
	}
	offset, err := pager.osfile.Seek(int64(pageNum*PageSize), io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek %v", err)
	}
	if offset == -1 {
		return fmt.Errorf("offset %v", offset)
	}
	originByte := make([]byte, realNum)
	q := (*[PageSize]byte)(pager.pages[pageNum])
	copy(originByte[0:realNum], (*q)[0:realNum])
	// 写入到byte指针里面
	bytesWritten, err := pager.osfile.WriteAt(originByte, offset)
	if err != nil {
		return fmt.Errorf("write %v", err)
	}
	// 捞取byte数组到这一页中
	fmt.Println("already wittern", bytesWritten)
	return nil
}

// todo 确认一下
//func unsfaeToSlice(pointer unsafe.Pointer, num int32)[]byte {
//	//originByte := make([]byte, num)
//	// q := (*[num]byte)(pointer)
//}

// executeStatement 实行sql语句 功能对应我们的虚拟机
func executeStatement(statement *Statement, table *Table)  executeResult{
	switch statement.statementType {
		case statementInsert:
			return executeInsert(statement, table)
	case statementSelect:
		return executeSelect(statement, table)
	default:
		fmt.Println("unknown statement")
	}
	return ExecuteSuccess
}

// todo 后面搞成通用类型
func pagerOpen(fileName string)(*Pager, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	fileLenth, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}
	if fileLenth % PageSize != 0 {
		fmt.Printf("Db file is not a whole number of pages. Corrupt file.\n");
		os.Exit(0)
	}
	return &Pager{
		pages:      make([]unsafe.Pointer, TABLE_MAX_PAGES),
		osfile:     file,
		numPages: uint32(fileLenth/PageSize),
		fileLength: fileLenth,
	}, nil
}

func dbOpen(fileName string) (*Table, error){
	pager, err := pagerOpen(fileName)
	if err != nil {
		return nil, err
	}
	table := newTable()
	table.pager = pager
	table.rootPageNum = 0
	if pager.numPages == 0 {
		// New database file. Initialize page 0 as leaf node.
		rootNode := getPage(table.pager, 0)
		c := (*uint8)(rootNode)
		fmt.Printf("c %v",c )
		initialize_leaf_node(rootNode)
	}
	return table, nil
}

func newTable() * Table{
	table := &Table{}
	//table.pager = make([]unsafe.Pointer, TABLE_MAX_PAGES, TABLE_MAX_PAGES)
	return table
}
// run main 主函数，这样写方便单元测试
func run()  {
	table, err := dbOpen("./db.txt")
	if err != nil {
		panic(err)
	}
	for  {
		printPrompt()
		// 语句解析
		inputBuffer, err := readInput()
		if err != nil {
			fmt.Println("read err", err)
		}
		// 特殊操作
		if len(inputBuffer) != 0 && inputBuffer[0] == '.' {
			switch doMetaCommand(inputBuffer, table) {
			case metaCommandSuccess:
				continue
			case metaCommandUnRecongnizedCommand:
				fmt.Println("Unrecognized command", inputBuffer)
				continue
			}
		}
		// 普通操作 code Generator
		statement := Statement{}
		switch prepareStatement(inputBuffer, &statement) {
		case prepareSuccess:
			break;
		case prepareUnrecognizedStatement:
			fmt.Println("Unrecognized keyword at start of ", inputBuffer)
			continue
		default:
			fmt.Println("invalid unput ", inputBuffer)
			continue
		}
		res := executeStatement(&statement, table)
		if res == ExecuteSuccess {
			fmt.Println("Exected")
			continue
		}
		if res == ExecuteTableFull {
			fmt.Printf("Error: Table full.\n");
			break
		}
		if res == EXECUTE_DUPLICATE_KEY {
			fmt.Printf("Error: Duplicate key.\n");
			break;
		}
	}
}

func main() {
	run()
}
