export interface PaginationState {
  pageIndex: number;
  pageSize: number;
}

export interface PaginationConfig {
  defaultPageSize?: number;
  minPageSize?: number;
  maxPageSize?: number;
}

export interface PaginationResult {
  pageIndex: number;
  pageSize: number;
  offset: number;
  limit: number;
  setPageIndex: (pageIndex: number) => void;
  setPageSize: (pageSize: number) => void;
  nextPage: () => void;
  previousPage: () => void;
  goToPage: (pageIndex: number) => void;
}

