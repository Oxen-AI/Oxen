import { useState, useEffect, useRef, useCallback } from 'react';
import { listen } from '@tauri-apps/api/event';
import type { IUploadProgressEvent } from '@/types';

interface UploadState {
  progress: IUploadProgressEvent | null;
  percentage: number;
  speed: number; // bytes per second
  eta: number; // seconds
}

export function useUpload() {
  const [state, setState] = useState<UploadState>({
    progress: null,
    percentage: 0,
    speed: 0,
    eta: 0,
  });

  const lastUpdateRef = useRef<{ time: number; bytes: number } | null>(null);
  const smoothSpeedRef = useRef<number>(0);

  const reset = useCallback(() => {
    setState({ progress: null, percentage: 0, speed: 0, eta: 0 });
    lastUpdateRef.current = null;
    smoothSpeedRef.current = 0;
  }, []);

  useEffect(() => {
    const EMA_ALPHA = 0.3; // weight for new sample; 0.7 carried from previous

    const unlisten = listen<IUploadProgressEvent>('upload-progress', (event) => {
      const p = event.payload;
      const now = Date.now();

      let speed = 0;
      let eta = 0;

      if (lastUpdateRef.current && p.bytesUploaded > lastUpdateRef.current.bytes) {
        const dt = (now - lastUpdateRef.current.time) / 1000;
        const db = p.bytesUploaded - lastUpdateRef.current.bytes;
        if (dt > 0) {
          const instantSpeed = db / dt;
          speed = smoothSpeedRef.current === 0
            ? instantSpeed
            : EMA_ALPHA * instantSpeed + (1 - EMA_ALPHA) * smoothSpeedRef.current;
          smoothSpeedRef.current = speed;
          const remaining = p.bytesTotal - p.bytesUploaded;
          eta = speed > 0 ? remaining / speed : 0;
        }
      }

      lastUpdateRef.current = { time: now, bytes: p.bytesUploaded };

      const percentage = p.bytesTotal > 0 ? (p.bytesUploaded / p.bytesTotal) * 100 : 0;

      setState({ progress: p, percentage, speed, eta });
    });

    return () => {
      unlisten.then((fn) => fn());
    };
  }, []);

  return { ...state, reset };
}
