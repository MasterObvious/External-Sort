# External-Sort
An Implementation of External Sort written in Java as part of the Second Year Java course. External sort is a sorting algorithm that is able to sort more items than can fit in working memory.

This implementation works by splitting the file into "chunks". Each chunk is small enough to fit into memory and is sorted through a very efficient radix sort where 2 bytes are treated as the "digit". Once each chunk in the file has been sorted, we merge them together using an _n_-way merge sort. (_n_ is determined by the number of chunks in the file).

This solution ranked 18<sup>th</sup> out of 95 submissions and was highly commended.
