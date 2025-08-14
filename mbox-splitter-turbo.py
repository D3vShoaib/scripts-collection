import sys
import os
from tqdm import tqdm

if len(sys.argv) != 3:
    print("\nUsage: python mbox-splitter-turbo.py filename.mbox size")
    print("       where `size` is a positive integer in MB\n")
    sys.exit()

filename = sys.argv[1]
if not os.path.exists(filename):
    print(f"File `{filename}` does not exist.")
    sys.exit()

try:
    split_size = int(sys.argv[2]) * 1024 * 1024  # Convert MB to Bytes
except ValueError:
    print("Size must be a positive number")
    sys.exit()

if split_size < 1:
    print("Size must be a positive number")
    sys.exit()

chunk_count = 1
output = filename.replace('.mbox', f'_{chunk_count}.mbox')
if os.path.exists(output):
    print(f"The file `{filename}` has already been split. Delete chunks to continue.")
    sys.exit()

print(f"Splitting `{filename}` into chunks of {sys.argv[2]} MB...")

# Get file size for progress tracking
file_size = os.path.getsize(filename)
print(f"File size: {file_size / (1024*1024*1024):.2f} GB")

# Initialize variables
current_chunk_size = 0
message_count = 0
total_messages = 0

# Open files
input_file = open(filename, 'rb')
output_file = open(output, 'wb')

# Initialize progress bar
pbar = tqdm(total=file_size, desc="Processing", unit="B", unit_scale=True, dynamic_ncols=True)

# Use larger read buffer for maximum speed
READ_SIZE = 1024 * 1024  # 10MB read buffer
bytes_processed = 0
current_message_size = 0

# Pre-compile the "From " pattern as bytes for faster comparison
FROM_PATTERN = b'From '

try:
    buffer = b""
    
    while True:
        # Read data in large chunks
        data = input_file.read(READ_SIZE)
        if not data:
            break
        
        buffer += data
        bytes_processed += len(data)
        pbar.update(len(data))
        
        # Process lines more efficiently
        start = 0
        while True:
            # Find next newline
            newline_pos = buffer.find(b'\n', start)
            if newline_pos == -1:
                # No more complete lines in buffer
                buffer = buffer[start:]
                break
            
            line = buffer[start:newline_pos + 1]
            start = newline_pos + 1
            
            # Quick check for "From " at start of line (much faster than regex)
            if line.startswith(FROM_PATTERN) and len(line) > 5 and line[4] == 32:  # 32 is space
                # If we have accumulated a message and it would exceed the chunk size
                if current_message_size > 0 and current_chunk_size + current_message_size >= split_size and message_count > 0:
                    # Close current chunk and start new one
                    output_file.close()
                    pbar.write(f"Chunk {chunk_count}: {current_chunk_size // (1024 * 1024)}MB, {message_count} messages")
                    
                    chunk_count += 1
                    output = filename.replace('.mbox', f'_{chunk_count}.mbox')
                    output_file = open(output, 'wb')
                    current_chunk_size = 0
                    message_count = 0
                
                # Count the previous message
                if current_message_size > 0:
                    message_count += 1
                    total_messages += 1
                    current_chunk_size += current_message_size
                
                current_message_size = len(line)
            else:
                # Continue current message
                current_message_size += len(line)
            
            # Write line to output
            output_file.write(line)
    
    # Handle any remaining data in buffer
    if buffer:
        current_message_size += len(buffer)
        output_file.write(buffer)
    
    # Count the last message
    if current_message_size > 0:
        message_count += 1
        total_messages += 1
        current_chunk_size += current_message_size

    # Close files
    output_file.close()
    input_file.close()
    pbar.close()

    print(f"\nFinal chunk {chunk_count}: {current_chunk_size // (1024 * 1024)}MB, {message_count} messages")
    print(f"Done! Total messages: {total_messages}, Total chunks: {chunk_count}")

except KeyboardInterrupt:
    print("\n\nOperation cancelled by user.")
    if 'output_file' in locals() and not output_file.closed:
        output_file.close()
    if 'input_file' in locals() and not input_file.closed:
        input_file.close()
    if 'pbar' in locals():
        pbar.close()
    sys.exit(1)
except Exception as e:
    print(f"\nError processing file: {e}")
    if 'output_file' in locals() and not output_file.closed:
        output_file.close()
    if 'input_file' in locals() and not input_file.closed:
        input_file.close()
    if 'pbar' in locals():
        pbar.close()
    sys.exit(1) 