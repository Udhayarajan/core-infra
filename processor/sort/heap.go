package sort

import "core-infra/common"

type Item struct {
	value *common.CSV
	file  int
}

type MinHeap struct {
	items []*Item
	less  func(a, b *common.CSV) bool
}

func (h *MinHeap) Len() int { return len(h.items) }

func (h *MinHeap) Less(i, j int) bool {
	return h.less(h.items[i].value, h.items[j].value)
}

func (h *MinHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *MinHeap) Push(x any) {
	h.items = append(h.items, x.(*Item))
}

func (h *MinHeap) Pop() any {
	n := len(h.items)
	item := h.items[n-1]
	h.items = h.items[:n-1]
	return item
}
