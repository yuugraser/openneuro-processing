"""
Neural data processing pipeline for iEEG data.
Implements advanced signal processing and analysis techniques for BIDS format.
"""
import mne
import numpy as np
import pandas as pd
from mne_bids import BIDSPath, read_raw_bids
from scipy import signal
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import json
from concurrent.futures import ProcessPoolExecutor
from config import FILTER_RANGES, SAMPLING_RATES
import logging

class IEEGProcessor:
    def __init__(self, sampling_rate: int = 1000):
        """
        Initialize IEEG processor with sampling rate.
        """
        self.sampling_rate = sampling_rate
        self.nyquist = sampling_rate / 2

    def process_dataset(self, file_paths: List[str],
                       process_config: Dict,
                       progress_callback: Optional[callable] = None) -> Dict:
        """
        Process multiple iEEG files.

        Args:
            file_paths: List of file paths (as strings)
            process_config: Processing configuration
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary containing processed results
        """
        results = {}

        # Convert string paths to Path objects
        paths = [Path(fp) for fp in file_paths]

        # Group files by subject/task
        ieeg_files = {}
        for path in paths:
            path_str = str(path)
            if path_str.endswith('_ieeg.tsv'):
                base_name = path_str.replace('_ieeg.tsv', '')
                ieeg_files[base_name] = {
                    'data': path,
                    'json': None,
                    'channels': None,
                    'events': None
                }

        # Find associated files
        for path in paths:
            path_str = str(path)
            base_name = path_str.rsplit('_', 1)[0]  # Remove the last component
            if base_name in ieeg_files:
                if path_str.endswith('_ieeg.json'):
                    ieeg_files[base_name]['json'] = path
                elif path_str.endswith('_channels.tsv'):
                    ieeg_files[base_name]['channels'] = path
                elif path_str.endswith('_events.tsv'):
                    ieeg_files[base_name]['events'] = path

        # Process each set of files
        for base_name, files in ieeg_files.items():
            if progress_callback:
                progress_callback(f"Processing {Path(base_name).name}")

            try:
                # Read the data
                data = pd.read_csv(files['data'], sep='\t')

                # Read metadata if available
                metadata = None
                if files['json']:
                    with open(files['json'], 'r') as f:
                        metadata = json.load(f)

                # Read channel information if available
                channels = None
                if files['channels']:
                    channels = pd.read_csv(files['channels'], sep='\t')

                # Read events if available
                events = None
                if files['events']:
                    events = pd.read_csv(files['events'], sep='\t')

                # Store the results
                results[base_name] = {
                    'data': data.to_dict(),
                    'metadata': metadata,
                    'channels': channels.to_dict() if channels is not None else None,
                    'events': events.to_dict() if events is not None else None
                }

                # Perform analyses if we have both data and metadata
                if metadata and 'SamplingFrequency' in metadata:
                    sampling_rate = metadata['SamplingFrequency']

                    # Convert data to numpy array for processing
                    signal_data = data.iloc[:, 1:].to_numpy().T  # Assuming first column is time

                    if process_config.get('power_spectra', True):
                        freqs, psd = signal.welch(signal_data,
                                                fs=sampling_rate,
                                                nperseg=int(sampling_rate * 2))
                        results[base_name]['power_spectra'] = {
                            'frequencies': freqs.tolist(),
                            'psd': psd.tolist()
                        }

                    if process_config.get('band_powers', True):
                        band_powers = {}
                        for band_name, (fmin, fmax) in FILTER_RANGES.items():
                            # Design bandpass filter
                            b, a = signal.butter(4, [fmin, fmax],
                                               btype='band',
                                               fs=sampling_rate)
                            # Apply filter and compute power
                            filtered = signal.filtfilt(b, a, signal_data)
                            power = np.mean(filtered ** 2, axis=1)
                            band_powers[band_name] = power.tolist()
                        results[base_name]['band_powers'] = band_powers

                    if process_config.get('connectivity', True):
                        # Compute connectivity for a subset of the data
                        window_size = int(sampling_rate * 60)  # 1 minute window
                        if signal_data.shape[1] > window_size:
                            data_window = signal_data[:, :window_size]
                        else:
                            data_window = signal_data

                        n_channels = data_window.shape[0]
                        connectivity = np.zeros((n_channels, n_channels))

                        for i in range(n_channels):
                            for j in range(i + 1, n_channels):
                                # Compute correlation
                                corr = np.corrcoef(data_window[i], data_window[j])[0, 1]
                                connectivity[i, j] = corr
                                connectivity[j, i] = corr

                        results[base_name]['connectivity'] = connectivity.tolist()

            except Exception as e:
                logging.error(f"Error processing {base_name}: {str(e)}")
                if progress_callback:
                    progress_callback(f"Error processing {Path(base_name).name}: {str(e)}")
                continue

        return results