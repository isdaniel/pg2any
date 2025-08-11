//! Buffer utilities for reading and writing replication protocol messages
//! 
//! This module provides safe wrappers for reading and writing binary data
//! in PostgreSQL's logical replication protocol format.

use crate::error::{CdcError, Result};

/// Buffer reader for parsing binary protocol messages
pub struct BufferReader<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> BufferReader<'a> {
    /// Create a new buffer reader
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    /// Get current position in the buffer
    pub fn position(&self) -> usize {
        self.position
    }

    /// Get remaining bytes in the buffer
    pub fn remaining(&self) -> usize {
        if self.position >= self.data.len() {
            0
        } else {
            self.data.len() - self.position
        }
    }

    /// Check if there are enough bytes remaining
    fn ensure_bytes(&self, count: usize) -> Result<()> {
        if self.remaining() < count {
            return Err(CdcError::protocol(format!(
                "Not enough bytes remaining. Need {}, have {}",
                count,
                self.remaining()
            )));
        }
        Ok(())
    }

    /// Skip the message type byte and return current position
    pub fn skip_message_type(&mut self) -> Result<usize> {
        self.ensure_bytes(1)?;
        self.position += 1;
        Ok(self.position)
    }

    /// Read a single byte
    pub fn read_u8(&mut self) -> Result<u8> {
        self.ensure_bytes(1)?;
        let value = self.data[self.position];
        self.position += 1;
        Ok(value)
    }

    /// Read a 16-bit unsigned integer in network byte order
    pub fn read_u16(&mut self) -> Result<u16> {
        self.ensure_bytes(2)?;
        let bytes = [self.data[self.position], self.data[self.position + 1]];
        self.position += 2;
        Ok(u16::from_be_bytes(bytes))
    }

    /// Read a 32-bit unsigned integer in network byte order
    pub fn read_u32(&mut self) -> Result<u32> {
        self.ensure_bytes(4)?;
        let bytes = [
            self.data[self.position],
            self.data[self.position + 1],
            self.data[self.position + 2],
            self.data[self.position + 3],
        ];
        self.position += 4;
        Ok(u32::from_be_bytes(bytes))
    }

    /// Read a 64-bit unsigned integer in network byte order
    pub fn read_u64(&mut self) -> Result<u64> {
        self.ensure_bytes(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[self.position..self.position + 8]);
        self.position += 8;
        Ok(u64::from_be_bytes(bytes))
    }

    /// Read a 16-bit signed integer in network byte order
    pub fn read_i16(&mut self) -> Result<i16> {
        self.ensure_bytes(2)?;
        let bytes = [self.data[self.position], self.data[self.position + 1]];
        self.position += 2;
        Ok(i16::from_be_bytes(bytes))
    }

    /// Read a 32-bit signed integer in network byte order
    pub fn read_i32(&mut self) -> Result<i32> {
        self.ensure_bytes(4)?;
        let bytes = [
            self.data[self.position],
            self.data[self.position + 1],
            self.data[self.position + 2],
            self.data[self.position + 3],
        ];
        self.position += 4;
        Ok(i32::from_be_bytes(bytes))
    }

    /// Read a 64-bit signed integer in network byte order
    pub fn read_i64(&mut self) -> Result<i64> {
        self.ensure_bytes(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[self.position..self.position + 8]);
        self.position += 8;
        Ok(i64::from_be_bytes(bytes))
    }

    /// Read a null-terminated string
    pub fn read_cstring(&mut self) -> Result<String> {
        let start = self.position;
        while self.position < self.data.len() && self.data[self.position] != 0 {
            self.position += 1;
        }

        if self.position >= self.data.len() {
            return Err(CdcError::protocol(
                "Unterminated string in buffer".to_string(),
            ));
        }

        let result = String::from_utf8(self.data[start..self.position].to_vec())
            .map_err(|e| CdcError::protocol(format!("Invalid UTF-8 in string: {}", e)))?;

        self.position += 1; // Skip null terminator
        Ok(result)
    }

    /// Read a fixed-length string without null terminator
    pub fn read_string(&mut self, length: usize) -> Result<String> {
        self.ensure_bytes(length)?;
        let result = String::from_utf8(self.data[self.position..self.position + length].to_vec())
            .map_err(|e| CdcError::protocol(format!("Invalid UTF-8 in string: {}", e)))?;
        self.position += length;
        Ok(result)
    }

    /// Read raw bytes
    pub fn read_bytes(&mut self, length: usize) -> Result<Vec<u8>> {
        self.ensure_bytes(length)?;
        let result = self.data[self.position..self.position + length].to_vec();
        self.position += length;
        Ok(result)
    }

    /// Peek at the next byte without advancing position
    pub fn peek_u8(&self) -> Result<u8> {
        self.ensure_bytes(1)?;
        Ok(self.data[self.position])
    }

    /// Skip n bytes
    pub fn skip(&mut self, count: usize) -> Result<()> {
        self.ensure_bytes(count)?;
        self.position += count;
        Ok(())
    }
}

/// Buffer writer for creating binary protocol messages
pub struct BufferWriter<'a> {
    data: &'a mut [u8],
    position: usize,
}

impl<'a> BufferWriter<'a> {
    /// Create a new buffer writer
    pub fn new(data: &'a mut [u8]) -> Self {
        Self { data, position: 0 }
    }

    /// Get current position in the buffer
    pub fn position(&self) -> usize {
        self.position
    }

    /// Get bytes written so far
    pub fn bytes_written(&self) -> usize {
        self.position
    }

    /// Check if there's enough space for writing
    fn ensure_space(&self, count: usize) -> Result<()> {
        if self.position + count > self.data.len() {
            return Err(CdcError::buffer(format!(
                "Not enough space in buffer. Need {}, have {}",
                count,
                self.data.len() - self.position
            )));
        }
        Ok(())
    }

    /// Write a single byte
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.ensure_space(1)?;
        self.data[self.position] = value;
        self.position += 1;
        Ok(())
    }

    /// Write a 16-bit unsigned integer in network byte order
    pub fn write_u16(&mut self, value: u16) -> Result<()> {
        self.ensure_space(2)?;
        let bytes = value.to_be_bytes();
        self.data[self.position..self.position + 2].copy_from_slice(&bytes);
        self.position += 2;
        Ok(())
    }

    /// Write a 32-bit unsigned integer in network byte order
    pub fn write_u32(&mut self, value: u32) -> Result<()> {
        self.ensure_space(4)?;
        let bytes = value.to_be_bytes();
        self.data[self.position..self.position + 4].copy_from_slice(&bytes);
        self.position += 4;
        Ok(())
    }

    /// Write a 64-bit unsigned integer in network byte order
    pub fn write_u64(&mut self, value: u64) -> Result<()> {
        self.ensure_space(8)?;
        let bytes = value.to_be_bytes();
        self.data[self.position..self.position + 8].copy_from_slice(&bytes);
        self.position += 8;
        Ok(())
    }

    /// Write a 32-bit signed integer in network byte order
    pub fn write_i32(&mut self, value: i32) -> Result<()> {
        self.ensure_space(4)?;
        let bytes = value.to_be_bytes();
        self.data[self.position..self.position + 4].copy_from_slice(&bytes);
        self.position += 4;
        Ok(())
    }

    /// Write a 64-bit signed integer in network byte order
    pub fn write_i64(&mut self, value: i64) -> Result<()> {
        self.ensure_space(8)?;
        let bytes = value.to_be_bytes();
        self.data[self.position..self.position + 8].copy_from_slice(&bytes);
        self.position += 8;
        Ok(())
    }

    /// Write raw bytes
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.ensure_space(bytes.len())?;
        self.data[self.position..self.position + bytes.len()].copy_from_slice(bytes);
        self.position += bytes.len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_reader_basic() {
        let data = [0x01, 0x02, 0x03, 0x04];
        let mut reader = BufferReader::new(&data);

        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert_eq!(reader.read_u8().unwrap(), 0x02);
        assert_eq!(reader.remaining(), 2);
        assert_eq!(reader.position(), 2);
    }

    #[test]
    fn test_buffer_reader_u16() {
        let data = [0x01, 0x02];
        let mut reader = BufferReader::new(&data);
        assert_eq!(reader.read_u16().unwrap(), 0x0102);
    }

    #[test]
    fn test_buffer_reader_u32() {
        let data = [0x01, 0x02, 0x03, 0x04];
        let mut reader = BufferReader::new(&data);
        assert_eq!(reader.read_u32().unwrap(), 0x01020304);
    }

    #[test]
    fn test_buffer_writer_basic() {
        let mut data = [0u8; 8];
        {
            let mut writer = BufferWriter::new(&mut data);

            writer.write_u8(0x01).unwrap();
            writer.write_u16(0x0203).unwrap();
            writer.write_u32(0x04050607).unwrap();
            
            assert_eq!(writer.bytes_written(), 7);
        }

        assert_eq!(data, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x00]);
    }
}
