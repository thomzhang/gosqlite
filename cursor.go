package main

import "unsafe"

// Cursor 光标
type Cursor struct {
	table        *Table
	pageNum      uint32 // 第几页
	cellNum      uint32 // 多少个数据单元
	endOfTable bool
}

func tableStart(table *Table)  * Cursor{
	rootNode := getPage(table.pager, table.rootPageNum)
	numCells := *leaf_node_num_cells(rootNode)
	return &Cursor{
		table:      table,
		pageNum:    table.rootPageNum,
		cellNum:    0,
		endOfTable: numCells ==0,
	}
}

func cursorAdvance(cursor *Cursor) {
	node := getPage(cursor.table.pager, cursor.pageNum)
	cursor.cellNum += 1
	if cursor.cellNum >=(*leaf_node_num_cells(node)) {
		cursor.endOfTable = true
	}
}

func tableEnd(table *Table) *Cursor  {
	root_node := getPage(table.pager, table.rootPageNum)
	num_cells := *leaf_node_num_cells(root_node)
	return &Cursor{
		table:      table,
		pageNum:    table.rootPageNum,
		endOfTable: true,
		cellNum:    num_cells,
	}
}

// cursor_value 获取当前叶子节点的指针
func cursorValue(cursor *Cursor)  unsafe.Pointer {
	page := getPage(cursor.table.pager, cursor.pageNum)
	return  leaf_node_value(page, cursor.cellNum)
}

