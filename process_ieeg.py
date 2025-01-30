"""
Neural data processing pipeline for iEEG data.
Implements advanced signal processing and analysis techniques.
"""
import mne
import numpy as np
from scipy import signal
from typing import Dict, List, Tuple, Optional
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from config import FILTER_RANGES, SAMPLING_RATES


class IEEGProcessor:
    def __init__(self, sampling_rate: int = 1000):
        """
        Initialize IEEG processor with sampling rate.

        Args:
            sampling_rate: Sampling rate in Hz
        """
        self.sampling_rate = sampling_rate
        self.nyquist = sampling_rate / 2

    def preprocess_data(self, raw_data: mne.io.Raw) -> mne.io.Raw:
        """
        Preprocess raw iEEG data.

        Args:
            raw_data: Raw MNE data object

        Returns:
            Preprocessed MNE data object
        """
        # Notch filter for line noise
        raw_filtered = raw_data.notch_filter(
            freqs=[50, 100],  # Assuming 50Hz power line
            picks='eeg'
        )

        # High-pass filter to remove DC offset and drift
        raw_filtered = raw_filtered.filter(
            l_freq=0.5,
            h_freq=None,
            picks='eeg'
        )

        return raw_filtered

    def remove_artifacts(self, data: mne.io.Raw,
                         threshold: float = 5.0) -> mne.io.Raw:
        """
        Remove artifacts using ICA and amplitude thresholding.

        Args:
            data: MNE Raw object
            threshold: Z-score threshold for artifact rejection

        Returns:
            Cleaned MNE Raw object
        """
        # ICA for eye blink and muscle artifact removal
        ica = mne.preprocessing.ICA(n_components=0.95, random_state=42)
        ica.fit(data)

        # Detect and remove eye blink components
        eog_indices, _ = ica.find_bads_eog(data)
        ica.exclude = eog_indices

        # Apply ICA cleaning
        cleaned_data = data.copy()
        ica.apply(cleaned_data)

        # Amplitude thresholding
        signals = cleaned_data.get_data()
        z_scores = np.abs(signal.zscore(signals, axis=1))
        bad_indices = np.where(z_scores > threshold)

        # Interpolate bad segments
        signals[bad_indices] = np.nan
        signals = pd.DataFrame(signals).interpolate(method='linear').values

        cleaned_data._data = signals
        return cleaned_data

    def compute_power_spectra(self, data: mne.io.Raw,
                              fmin: float = 0.5,
                              fmax: float = 100.0) -> Tuple[np.ndarray, np.ndarray]:
        """
        Compute power spectra using Welch's method.

        Args:
            data: MNE Raw object
            fmin: Minimum frequency
            fmax: Maximum frequency

        Returns:
            Tuple of frequencies and power spectra
        """
        signals = data.get_data()
        freqs, psd = signal.welch(
            signals,
            fs=self.sampling_rate,
            nperseg=self.sampling_rate * 2,
            noverlap=self.sampling_rate,
            fmin=fmin,
            fmax=fmax
        )
        return freqs, psd

    def compute_connectivity(self, data: mne.io.Raw) -> np.ndarray:
        """
        Compute phase-locking value between channels.

        Args:
            data: MNE Raw object

        Returns:
            Connectivity matrix
        """
        signals = data.get_data()
        n_channels = signals.shape[0]
        plv_matrix = np.zeros((n_channels, n_channels))

        # Compute analytic signal using Hilbert transform
        analytic_signals = signal.hilbert(signals)
        phases = np.angle(analytic_signals)

        # Compute PLV for each channel pair
        for i in range(n_channels):
            for j in range(i + 1, n_channels):
                phase_diff = phases[i] - phases[j]
                plv = np.abs(np.mean(np.exp(1j * phase_diff)))
                plv_matrix[i, j] = plv
                plv_matrix[j, i] = plv

        return plv_matrix

    def process_file(self, file_path: Path,
                     process_config: Dict) -> Dict[str, np.ndarray]:
        """
        Process a single iEEG file with specified configuration.

        Args:
            file_path: Path to iEEG file
            process_config: Configuration dictionary for processing

        Returns:
            Dictionary containing processed data
        """
        # Load data
        raw = mne.io.read_raw(file_path, preload=True)

        # Preprocess
        cleaned_data = self.preprocess_data(raw)
        if process_config.get('remove_artifacts', True):
            cleaned_data = self.remove_artifacts(cleaned_data)

        results = {}

        # Compute specified analyses
        if process_config.get('power_spectra', True):
            freqs, psd = self.compute_power_spectra(cleaned_data)
            results['frequencies'] = freqs
            results['power_spectra'] = psd

        if process_config.get('connectivity', True):
            connectivity = self.compute_connectivity(cleaned_data)
            results['connectivity'] = connectivity

        # Compute band powers
        if process_config.get('band_powers', True):
            band_powers = {}
            for band_name, (fmin, fmax) in FILTER_RANGES.items():
                band_data = cleaned_data.copy().filter(
                    l_freq=fmin,
                    h_freq=fmax,
                    picks='eeg'
                )
                band_powers[band_name] = np.mean(band_data.get_data() ** 2, axis=1)
            results['band_powers'] = band_powers

        return results

    def process_dataset(self, file_paths: List[Path],
                        process_config: Dict,
                        progress_callback: Optional[callable] = None) -> Dict:
        """
        Process multiple iEEG files in parallel.

        Args:
            file_paths: List of file paths
            process_config: Processing configuration
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary containing processed results
        """
        results = {}

        def process_single_file(file_path: Path) -> Tuple[str, Dict]:
            result = self.process_file(file_path, process_config)
            if progress_callback:
                progress_callback(str(file_path))
            return str(file_path), result

        with ProcessPoolExecutor() as executor:
            for file_path, result in executor.map(process_single_file, file_paths):
                results[file_path] = result

        return results