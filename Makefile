.PHONY: clean

CFLAGS  := -Wall -std=c99 -o2 -g
CPPFLAGS  := -Wall -std=c99 -o2 -g
LD      := g++
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := client server

all: ${APPS}

client: common.o client.o sock.o
	${LD} -o $@ $^ ${LDLIBS}

server: common.o server.o sock.o
	${LD} -o $@ $^ ${LDLIBS}

test-client: test-client.o common.o client.o
	${LD} -o $@ $^ ${LDLIBS}

test-server: test-server.o common.o server.o
	${LD} -o $@ $^ ${LDLIBS}
	
clean:
	rm -f *.o ${APPS}

