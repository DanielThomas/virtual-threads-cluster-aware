Experiments exploring the potential benefits of cluster aware scheduling/worker affinity for virtual threads. Developed against `24-loom+7-60`.

See https://mail.openjdk.org/pipermail/loom-dev/2024-September/007161.html for background.

Benchmarks run on an AWS EC2 `m7a.8xlarge` instance:

```
# JMH version: 1.37
# VM version: JDK 24-loom, OpenJDK 64-Bit Server VM, 24-loom+7-60
# VM invoker: /usr/lib/jvm/openjdk-24-loom/bin/java
# VM options: -Dfile.encoding=UTF-8 -Duser.country=US -Duser.language=en -Duser.variant
```

External submission throughput:
```
Benchmark                  (placement)   Mode  Cnt       Score       Error  Units
ExternalSubmission.submit          FJP  thrpt    5   64702.102 ±  1615.238  ops/s
ExternalSubmission.submit   CHOOSE_TWO  thrpt    5   79286.111 ±  1336.440  ops/s
ExternalSubmission.submit  ROUND_ROBIN  thrpt    5   86255.203 ±   729.912  ops/s
ExternalSubmission.submit      CURRENT  thrpt    5  102784.334 ± 13334.137  ops/s
```


```
Benchmark                                  (multiplier)  (scheduler)   Mode  Cnt     Score     Error    Units
VirtualThreadsSchedulerCacheStress.submit             1   CHOOSE_TWO  thrpt    5  4873.550 ± 290.117  ops/min (~1750% CPU)
VirtualThreadsSchedulerCacheStress.submit             1      DEFAULT  thrpt    5  3782.106 ±  69.471  ops/min (~2650% CPU)
VirtualThreadsSchedulerCacheStress.submit            10   CHOOSE_TWO  thrpt    5   453.875 ±  80.840  ops/min
VirtualThreadsSchedulerCacheStress.submit            10      DEFAULT  thrpt    5   324.959 ±  25.635  ops/min
VirtualThreadsSchedulerCacheStress.submit           100   CHOOSE_TWO  thrpt    5    44.731 ±  23.493  ops/min
VirtualThreadsSchedulerCacheStress.submit           100      DEFAULT  thrpt    5    37.416 ±   5.211  ops/min
```

```
processor	: 0
vendor_id	: AuthenticAMD
cpu family	: 25
model		: 17
model name	: AMD EPYC 9R14
stepping	: 1
microcode	: 0xa101148
cpu MHz		: 3698.291
cache size	: 1024 KB
physical id	: 0
siblings	: 32
core id		: 0
cpu cores	: 32
apicid		: 0
initial apicid	: 0
fpu		: yes
fpu_exception	: yes
cpuid level	: 16
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc rep_good nopl nonstop_tsc cpuid extd_apicid aperfmperf tsc_known_freq pni pclmulqdq monitor ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy cr8_legacy abm sse4a misalignsse 3dnowprefetch topoext perfctr_core invpcid_single ssbd perfmon_v2 ibrs ibpb stibp ibrs_enhanced vmmcall fsgsbase bmi1 avx2 smep bmi2 invpcid avx512f avx512dq rdseed adx smap avx512ifma clflushopt clwb avx512cd sha_ni avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves avx512_bf16 clzero xsaveerptr rdpru wbnoinvd arat avx512vbmi pku ospke avx512_vbmi2 gfni vaes vpclmulqdq avx512_vnni avx512_bitalg avx512_vpopcntdq rdpid flush_l1d
bugs		: sysret_ss_attrs spectre_v1 spectre_v2 spec_store_bypass srso
bogomips	: 5200.00
TLB size	: 3584 4K pages
clflush size	: 64
cache_alignment	: 64
address sizes	: 48 bits physical, 48 bits virtual
power management:
```
