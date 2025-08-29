# TODO
3) Choose the payload 
Fixed-size POD struct makes the most sense imo
struct POD{
    char data[k];
    uint16_t len;
    uint64_t send_ts_ns
};

4) Where the buffer sits
RX -> Parse: `recvfrom` threads produce raw messages; worker pool parses + records metrics.
Have dedicated threads for communicating with sender and dedicated threads to process received info.

5) Control the workload
Considering that my CPU is an i7 12gen with 16 virtual cores, I am not particularly sure what would be the most optimal distribution of threads or what throughput I should expect. I did some experimentation with 1e7 ints being requested to be enqueued by 8 producer threads and 8 consumer thread reading, and found that it took ~2s to complete with a buffer size of 1<<15;

So I will start with roughly 50,000 messages a second. However, that is without considering potential limitations to the sender client and my network.

6) Instrumentation 
- Put a rx_ts_ns at recfrom
- workers compute now - rx_ts_ns (end-to-end).

7) Make the buffer swappable without code drift
- Instead of templating the benchmark, just alter `using Buffer_t = mpmc_ring<Payload, N>;`

8) Phased build
Phase A (microbench, no sockets)
	I already have this kind of implemented within raw/main.cpp; however, I should
	change the payload type to the planned POD struct for more accurate measurement.

	I should make the FIX protocol generation and parsing its own class for better
	encapsulation and easier api use.

Phase B (real UDP, single group)
	One sender, one receiver, multiple threads for parsing and processing. Just
	to provide a foundation that will later scale with more data streams.

Phase C (real UDP, multi-group)
	RX thread (one per group) -> one shared MPMC -> M workers

9) A word on semantics (busy-wait vs drop)
Instead of waiting on enqueue/dequeue succeeds, drop packets that take to long to process to avoid stalling NIC.
