package api

import (
	"net/http"
	"strconv"
)

const (
	defaultPerPage = 25
	maxPerPage     = 100
)

type Pagination struct {
	Page    int  `json:"page"`
	PerPage int  `json:"per_page"`
	HasNext bool `json:"has_next"`
}

func parsePagination(r *http.Request) (limit, offset int, page int, perPage int) {
	q := r.URL.Query()
	page = 1
	perPage = defaultPerPage
	if v := q.Get("page"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 {
			page = p
		}
	}
	if v := q.Get("per_page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > maxPerPage { n = maxPerPage }
			perPage = n
		}
	}
	limit = perPage
	offset = (page - 1) * perPage
	return
}
