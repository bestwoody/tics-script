# CH TCP arrow transfer interface benchmark

## Summary
* Transfer interface speed:
    * 110MB/s per CPU core
    * Leaner increase when using more encoders (AKA, more CPU cores).
    * Almost reach max speed when `encoders == CPU cores`.
    * Add more encoders when `encoders >= CPU cores` will stand at the max speed.
* The best config of CHSpark:
    * Partitions: depended on Spark framework and CPU cores
    * Decoders: depended on paritions, 1 or 2 should be enough.
    * Encoders: same as CPU cores.

## Benchmark result
```
partitions:  1, decoders:  4, encoders:   1, bytes/s:  121108364
partitions:  2, decoders:  4, encoders:   1, bytes/s:  119618138
partitions:  4, decoders:  4, encoders:   1, bytes/s:  120713156
partitions:  8, decoders:  4, encoders:   1, bytes/s:  119859590
partitions: 16, decoders:  4, encoders:   1, bytes/s:  119922802
partitions: 24, decoders:  4, encoders:   1, bytes/s:  120855625
partitions: 32, decoders:  4, encoders:   1, bytes/s:  119665906
partitions: 64, decoders:  4, encoders:   1, bytes/s:  119158339

partitions:  1, decoders:  4, encoders:   2, bytes/s:  236709965
partitions:  2, decoders:  4, encoders:   2, bytes/s:  233187306
partitions:  4, decoders:  4, encoders:   2, bytes/s:  237502578
partitions:  8, decoders:  4, encoders:   2, bytes/s:  234274781
partitions: 16, decoders:  4, encoders:   2, bytes/s:  232243753
partitions: 24, decoders:  4, encoders:   2, bytes/s:  236188577
partitions: 32, decoders:  4, encoders:   2, bytes/s:  237060534
partitions: 64, decoders:  4, encoders:   2, bytes/s:  236900000

partitions:  1, decoders:  4, encoders:   4, bytes/s:  458702977
partitions:  2, decoders:  4, encoders:   4, bytes/s:  456479432
partitions:  4, decoders:  4, encoders:   4, bytes/s:  458882668
partitions:  8, decoders:  4, encoders:   4, bytes/s:  457235340
partitions: 16, decoders:  4, encoders:   4, bytes/s:  449152105
partitions: 24, decoders:  4, encoders:   4, bytes/s:  450452721
partitions: 32, decoders:  4, encoders:   4, bytes/s:  459163377
partitions: 64, decoders:  4, encoders:   4, bytes/s:  450599742

partitions:  1, decoders:  4, encoders:   8, bytes/s:  778967778
partitions:  2, decoders:  4, encoders:   8, bytes/s:  828280733
partitions:  4, decoders:  4, encoders:   8, bytes/s:  819136412
partitions:  8, decoders:  4, encoders:   8, bytes/s:  828369480
partitions: 16, decoders:  4, encoders:   8, bytes/s:  797727433
partitions: 24, decoders:  4, encoders:   8, bytes/s:  763384730
partitions: 32, decoders:  4, encoders:   8, bytes/s:  773984642
partitions: 64, decoders:  4, encoders:   8, bytes/s:  775148657

partitions:  4, decoders:  4, encoders:   2, bytes/s:  235983834
partitions:  4, decoders:  4, encoders:   4, bytes/s:  457782230
partitions:  4, decoders:  4, encoders:   8, bytes/s:  813525180
partitions:  2, decoders:  2, encoders:   8, bytes/s:  799550669
partitions:  4, decoders:  2, encoders:   8, bytes/s:  812149293
partitions:  2, decoders:  4, encoders:   8, bytes/s:  778003602
partitions:  1, decoders:  2, encoders:   8, bytes/s:  769064857
partitions:  1, decoders:  1, encoders:   8, bytes/s:  725837224
partitions:  2, decoders:  1, encoders:   8, bytes/s:  794464845
partitions:  4, decoders:  4, encoders:  16, bytes/s:  973047951
partitions:  4, decoders:  4, encoders:  32, bytes/s:  958606241
partitions:  4, decoders:  4, encoders:  64, bytes/s: 1011309020
partitions:  4, decoders:  4, encoders: 128, bytes/s:  971226611
```

## Env
* Intel Xeon E312xx 16 cores 2.4Ghz.
* 16G Memory 1600MHz .
* SSD 2.0GB+/s.
* CentOS Linux release 7.2.1511
