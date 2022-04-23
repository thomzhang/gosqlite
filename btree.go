package main

import (
	"fmt"
	"os"
	"unsafe"
)

type NodeType int32
const  (
	internalNode NodeType = 0
	leafNode NodeType = 1
)

// 公共的节点头
const (
	NODE_TYPE_SIZE = 1  // uint8的长度
	NODE_TYPE_OFFSET = 0
	IS_ROOT_SIZE = 1
	IS_ROOT_OFFSET = NODE_TYPE_SIZE
	PARENT_POINTER_SIZE = 4   //  sizeof(uint32_t);
	PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
	COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
)

// 叶子节点
const (
	LEAF_NODE_NUM_CELLS_SIZE =  4 //sizeof(uint32_t);
	LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
	LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
)

// Leaf Node Body Layout
const (
	LEAF_NODE_KEY_SIZE = 8 //sizeof(uint32_t);
	LEAF_NODE_KEY_OFFSET = 0
	LEAF_NODE_VALUE_SIZE = ROW_SIZE
	LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
	LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
	LEAF_NODE_SPACE_FOR_CELLS = PageSize - LEAF_NODE_HEADER_SIZE;
	LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
)

//  (*uint32)
func leaf_node_num_cells(node unsafe.Pointer) *uint32 {
	return (*uint32)((unsafe.Pointer)(uintptr(node) + uintptr(LEAF_NODE_NUM_CELLS_OFFSET)))
}

func leaf_node_cell(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return ((unsafe.Pointer)(uintptr(node) + uintptr(LEAF_NODE_NUM_CELLS_OFFSET) + uintptr(cellNum * LEAF_NODE_CELL_SIZE)))
}

func leaf_node_key(node unsafe.Pointer, cellNum uint32)*uint32 {
	return  (*uint32)(leaf_node_cell(node, cellNum))

}

func leaf_node_value(node unsafe.Pointer, cell_num uint32) (unsafe.Pointer) {
	return unsafe.Pointer(uintptr(leaf_node_cell(node, cell_num))+ uintptr(LEAF_NODE_KEY_SIZE))
}

// +void initialize_leaf_node(void* node) { *leaf_node_num_cells(node) = 0; }
func initialize_leaf_node(node unsafe.Pointer) {
	set_node_type(node, leafNode);
	num := leaf_node_num_cells(node)
	*num = 0
}

func set_node_type(node unsafe.Pointer,  nodeType NodeType) {
	value := uint8(nodeType);
	// value
	d := (*uint8)(node)
	fmt.Printf("d %v", d)
	c := (*uint8)(getoffsetNodeType(node))
	*c = value
}

func getoffsetNodeType(node unsafe.Pointer) unsafe.Pointer {
	return ((unsafe.Pointer)(uintptr(node) + uintptr(NODE_TYPE_OFFSET)))
}

// 创建一个将键/值插入节点的函数，它将以光标作为输入来插入对应的值
func leaf_node_insert(cursor *Cursor, key uint32, row *Row)  {
	node := getPage(cursor.table.pager, cursor.pageNum)
	num_cells := *leaf_node_num_cells(node)
	if num_cells >= LEAF_NODE_MAX_CELLS{
		fmt.Printf("Need to implement splitting a leaf node.\n");
		os.Exit(0)
	}
	if cursor.cellNum < num_cells {
		for i := num_cells; i > cursor.cellNum; i-- {
			memcpyNodeCell(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
		}
	}
	*(leaf_node_num_cells(node)) += 1;
	*(leaf_node_key(node, cursor.cellNum)) = key;
	serializeRow(row, leaf_node_value(node, cursor.cellNum));
}

// memcpyNodeCell 模拟memcpy
func memcpyNodeCell(source , destination unsafe.Pointer, num uint32) {
	sourceByte := (*[LEAF_NODE_CELL_SIZE]byte)(source)
	d := (*[LEAF_NODE_CELL_SIZE]byte)(destination)
	copy(sourceByte[0:num], d[0:num])
}

func print_constants()  {
	fmt.Printf("ROW_SIZE: %d\n", ROW_SIZE);
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

func print_leaf_node(node unsafe.Pointer) {
	num_cells := *leaf_node_num_cells(node);
	fmt.Printf("leaf (size %d)\n", num_cells);
	for i := uint32(0); i < num_cells; i++ {
		key := *leaf_node_key(node, i)
		fmt.Printf("  - %d : %d\n", i, key)
	}
}

// 返回key的位置，如果key不存在，返回应该被插入的位置
func tableFind(table *Table, key uint32) *Cursor {
	rootPageNum := table.rootPageNum
	rootNode := getPage(table.pager, rootPageNum)
	// 没有找到匹配到
	if getNodeType(rootNode) == leafNode {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		fmt.Printf("Need to implement searching an internal node\n");
		os.Exit(0);
	}
	return nil
}

func getNodeType(node unsafe.Pointer) NodeType {
	valueP := uintptr(node)+ uintptr(NODE_TYPE_OFFSET)
	curA := unsafe.Pointer(valueP)
	c := (*uint8)(curA)
	value := *c
	return NodeType(value)
}

func leafNodeFind(table *Table, pageNum uint32, key uint32) *Cursor {
	node := getPage(table.pager, pageNum)
	num_cells := *leaf_node_num_cells(node)
	cur := &Cursor{
		table:   table,
		pageNum: pageNum,
	}
	// Binary search
	var min_index uint32
	var one_past_max_index  = num_cells
	for ;one_past_max_index != min_index; {
		index := (min_index + one_past_max_index) /2
		key_at_index := *leaf_node_key(node, index)
		if key == key_at_index {
			cur.cellNum = index
			return cur
		}
		// 如果在小到一边，就将最大值变成当前索引
		if key < key_at_index {
			one_past_max_index = index
		} else {
			min_index = index+1    // 选择左侧
		}
	}
	cur.cellNum = min_index
	return cur
}
