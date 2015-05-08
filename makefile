
.PHONY : all

all: test

test: redis_cluster.c redis_cluster.h test.c
	gcc -g -Wall $^ -o $@ -lhiredis

.PHONY : clean
clean:
	rm -f *.o
	rm -f test
