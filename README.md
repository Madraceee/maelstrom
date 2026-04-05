# MAELSTROM Challenge

Solution for [Fly.io](https://fly.io/dist-sys/) distributed systems challenges.

Steps

Install Maelstrom
```
wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2
bzip2 -d  maelstrom.tar.bz2
tar -xvf maelstrom.tar
```

Build
```
go build -o ./bin/run
```

Run Test
```
./maelstrom/maelstrom test -w echo --bin ./bin/run
```

Refer to the website for more details on how to run tests
