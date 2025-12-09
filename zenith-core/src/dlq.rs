use crate::error::Result;
use crate::pipeline::transaction_buffer::Event;
use async_trait::async_trait;
use chrono::Utc;
use serde::Serialize;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Failed event wrapper for DLQ
#[derive(Debug, Serialize)]
pub struct FailedEvent {
    pub error: String,
    pub timestamp: String,
    #[serde(flatten)]
    pub event: Event,
}

/// Dead Letter Queue interface
#[async_trait]
pub trait DeadLetterQueue: Send + Sync {
    /// Save a failed event to the DLQ
    async fn is_enabled(&self) -> bool;
    async fn write(&self, event: Event, error: String) -> Result<()>;
    async fn write_batch(&self, events: Vec<Event>, error: String) -> Result<()>;
}

/// File-based Dead Letter Queue
pub struct FileDeadLetterQueue {
    enabled: bool,
    path: PathBuf,
    file: Arc<Mutex<Option<File>>>,
}

impl FileDeadLetterQueue {
    pub fn new(enabled: bool, path: impl AsRef<Path>) -> Self {
        Self {
            enabled,
            path: path.as_ref().to_path_buf(),
            file: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_file(&self) -> Result<tokio::sync::MutexGuard<'_, Option<File>>> {
        let mut file_guard = self.file.lock().await;
        
        if file_guard.is_none() {
            if let Some(parent) = self.path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| crate::Error::Io(e))?;
            }
            
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .await
                .map_err(|e| crate::Error::Io(e))?;
                
            *file_guard = Some(file);
        }
        
        Ok(file_guard)
    }
}

#[async_trait]
impl DeadLetterQueue for FileDeadLetterQueue {
    async fn is_enabled(&self) -> bool {
        self.enabled
    }

    async fn write(&self, event: Event, error: String) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let failed = FailedEvent {
            error,
            timestamp: Utc::now().to_rfc3339(),
            event,
        };

        let mut json = serde_json::to_vec(&failed).map_err(|e| crate::Error::Config(e.to_string()))?;
        json.push(b'\n');

        let mut file_guard = self.get_file().await?;
        if let Some(file) = file_guard.as_mut() {
            file.write_all(&json).await.map_err(|e| crate::Error::Io(e))?;
            file.flush().await.map_err(|e| crate::Error::Io(e))?;
        }

        Ok(())
    }

    async fn write_batch(&self, events: Vec<Event>, error: String) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        
        // Optimize by creating one big buffer? Or just loop. 
        // Loop is simpler for now, async IO handles batching somewhat at OS level usually, 
        // but explicit buffering would be better. 
        // For DLQ, safety > perf.
        
        let mut buffer = Vec::new();
        let ts = Utc::now().to_rfc3339();
        
        for event in events {
            let failed = FailedEvent {
                error: error.clone(),
                timestamp: ts.clone(),
                event,
            };
            let mut json = serde_json::to_vec(&failed).map_err(|e| crate::Error::Config(e.to_string()))?;
            json.push(b'\n');
            buffer.extend_from_slice(&json);
        }
        
        let mut file_guard = self.get_file().await?;
        if let Some(file) = file_guard.as_mut() {
            file.write_all(&buffer).await.map_err(|e| crate::Error::Io(e))?;
            file.flush().await.map_err(|e| crate::Error::Io(e))?;
        }
        
        Ok(())
    }
}
