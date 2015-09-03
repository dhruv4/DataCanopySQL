##a.out: pgC.c
##	gcc -c -I/usr/include/postgresql pgC.c -lm

CPPFLAGS += -I/usr/include/postgresql

CFLAGS   += -g

LDFLAGS  += -g

LDLIBS   += -L/usr/include/postgresql/lib -lpq -lm

pgC:  pgC.o