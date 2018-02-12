function run()
{
    cd ../spark-connector && ./chraw-scala.sh "select * from lineitem" "$1" "$2" "$3"
}

run 1 4 1
run 2 4 1
run 4 4 1
run 8 4 1
run 16 4 1
run 24 4 1
run 32 4 1
run 64 4 1

run 1 4 2
run 2 4 2
run 4 4 2
run 8 4 2
run 16 4 2
run 24 4 2
run 32 4 2
run 64 4 2

run 1 4 4
run 2 4 4
run 4 4 4
run 4 4 4
run 8 4 4
run 16 4 4
run 24 4 4
run 32 4 4
run 64 4 4

run 1 1 8
run 2 1 8
run 2 2 8
run 1 2 8
run 4 2 8
run 4 4 8
run 1 4 8
run 2 4 8
run 4 4 8
run 8 4 8
run 16 4 8
run 24 4 8
run 32 4 8
run 64 4 8

run 4 4 16
run 16 4 16
run 24 4 16
run 32 4 16
run 64 4 16

run 4 4 32
run 16 4 32
run 24 4 32
run 32 4 32
run 64 4 32

run 4 4 64
