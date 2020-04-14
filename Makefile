CC = gcc
EXECS = server client
CFLAGS = -g3 -Wall -Wextra -Wcast-qual -Wcast-align -g -Winline -Wfloat-equal
CFLAGS += -Wnested-externs -std=gnu99 -D_GNU_SOURCE -pthread
PROMPT = -DPROMPT
.PHONY = all clean
all: $(EXECS)
server: server.c db.c comm.c
	$(CC) $(CFLAGS) $(PROMPT) $^ -o $@
client: client.c
	$(CC) $(CFLAGS) $< -o $@
clean:
	rm -f *.o *~ $(EXECS)
