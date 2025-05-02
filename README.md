# Distributed Queue Design - Toy Project

## Goal

To build a distributed queue in Go, focusing on learning fundamental concepts of queues, durability, efficient disk persistence, and concurrent read/write for a small number of concurrent users (10-20).

## Core Components

1.  **Segmented Log:**
    * **Segmentation Strategy:** Size-based segmentation. New segments will be created once the current segment reaches a configurable maximum size.
    * **Segment File Naming:** Sequential numbering (e.g., `segment_00000.log`).
    * **Active Segment:** The segment with the highest number is the active segment for writing.
    * **Roll Over:** When the active segment reaches the maximum size, it's closed, a new segment file is created with the next sequential number, and this becomes the new active segment.

2.  **Data Format:**
    * Each entry in the log will consist of:
        * **Length Prefix:** A fixed number of bytes (e.g., 4 or 8) indicating the length of the following message payload.
        * **Message Payload:** The raw byte array enqueued by the producer. Consumers are responsible for serialization/deserialization.

3.  **In-Memory Offset-Based Index:**
    * A hash map (Go `map`) where:
        * **Key:** A unique, global message ID (e.g., an auto-incrementing integer).
        * **Value:** A struct/tuple containing the `Segment Number` and the `Byte Offset` within that segment where the message begins.
    * **Index Updates (on write):** When a new message is written:
        * A global ID is generated.
        * The current segment number and write offset are recorded and associated with the new global ID in the index.
        * The write offset is updated.

4.  **Queue Semantics - Sequential Consumption:**
    * **Enqueue:** Producers append messages (with length prefix) to the active segment. The index is updated with the message ID and offset.
    * **Dequeue (Sequential):**
        * Consumers will track their last successfully processed global message ID.
        * On startup, a consumer will retrieve its last processed ID. If it's the first time or all messages are processed, it starts from the beginning of the earliest segment.
        * To get the next message, the consumer finds the message with the ID immediately following its last processed ID using the index.
        * After processing, the consumer updates its last processed ID in persistent storage.

5.  **Tracking Last Read Position:**
    * **Mechanism:** Consumer-specific read offsets stored persistently.
    * **Storage:** A simple key-value store on disk (e.g., a file or a directory of files). The key is the consumer ID, and the value is the last processed global message ID.
    * **Update:** Consumers update their last processed ID in the storage after successfully processing a message.
    * **Initial Position:** New consumers start reading from the beginning of the log (the earliest segment).
    * **Consumer Identity:** Unique identifiers will be assigned to consumers.

## Further Considerations

* **Index Persistence:** How to handle restarts and rebuild the in-memory index (e.g., scanning logs or periodic snapshots).
* **Concurrency Control:** Ensuring thread-safe access to the log files and the in-memory index for concurrent readers and writers.
* **Error Handling:** What happens if a read or write operation fails?
* **Message Acknowledgment (Future):** For more robust delivery guarantees, we might consider adding acknowledgements from consumers.
* **Log Archiving/Deletion (Future):** How to manage older segments to prevent disk space issues.