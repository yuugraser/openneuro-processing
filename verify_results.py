import json
import boto3
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import seaborn as sns
from typing import Dict, Any


def load_results_from_s3(bucket_name: str, prefix: str) -> Dict[str, Any]:
    """
    Load the most recent processing results from S3.

    Args:
        bucket_name: Name of the S3 bucket
        prefix: Prefix path in the bucket

    Returns:
        Dictionary containing the results
    """
    # Initialize S3 client
    s3 = boto3.client('s3')

    # List objects with the given prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Find the most recent results file
    result_files = [obj for obj in response['Contents']
                    if obj['Key'].endswith('processing_results.json')]
    latest_file = max(result_files, key=lambda x: x['LastModified'])

    # Download and load the JSON
    response = s3.get_object(Bucket=bucket_name, Key=latest_file['Key'])
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)


def verify_and_visualize_results(results: Dict[str, Any], output_dir: str = 'verification_plots'):
    """
    Verify results and create visualization plots.

    Args:
        results: Dictionary containing processing results
        output_dir: Directory to save plots
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    for file_path, file_results in results.items():
        print(f"\nVerifying results for: {file_path}")

        # Create a subplot figure for each file
        fig = plt.figure(figsize=(15, 10))
        plt.suptitle(f"Analysis Results: {Path(file_path).name}")

        # 1. Power Spectra
        if 'spectral_analysis' in file_results:
            print("✓ Power spectra analysis present")
            plt.subplot(2, 2, 1)
            freqs = np.array(file_results['spectral_analysis']['frequencies'])
            psd = np.array(file_results['spectral_analysis']['power_spectra'])
            plt.semilogy(freqs, psd.mean(axis=0))
            plt.xlabel('Frequency (Hz)')
            plt.ylabel('Power Spectral Density')
            plt.title('Average Power Spectrum')
        else:
            print("✗ No power spectra analysis found")

        # 2. Band Powers
        if 'band_powers' in file_results:
            print("✓ Band powers analysis present")
            plt.subplot(2, 2, 2)
            bands = list(file_results['band_powers'].keys())
            powers = [np.mean(file_results['band_powers'][band]) for band in bands]
            plt.bar(bands, powers)
            plt.ylabel('Average Power')
            plt.title('Band Powers')
            plt.xticks(rotation=45)
        else:
            print("✗ No band powers analysis found")

        # 3. Connectivity
        if 'connectivity_analysis' in file_results:
            print("✓ Connectivity analysis present")
            plt.subplot(2, 2, 3)
            conn_matrix = np.array(file_results['connectivity_analysis'])
            sns.heatmap(conn_matrix, cmap='viridis')
            plt.title('Channel Connectivity')
        else:
            print("✗ No connectivity analysis found")

        # 4. Metadata verification
        if 'metadata' in file_results:
            print("✓ Metadata present")
            metadata = file_results['metadata']
            plt.subplot(2, 2, 4)
            plt.axis('off')
            metadata_text = '\n'.join([f"{k}: {v}"
                                       for k, v in metadata.items()
                                       if k in ['TaskName', 'SamplingFrequency']])
            plt.text(0.1, 0.5, f"Metadata:\n{metadata_text}",
                     fontsize=10, family='monospace')
        else:
            print("✗ No metadata found")

        # Save the figure
        plt.tight_layout()
        fig_path = output_path / f"verification_{Path(file_path).stem}.png"
        plt.savefig(fig_path)
        plt.close()

        print(f"Plots saved to: {fig_path}")


def main():
    # Configure these parameters
    BUCKET_NAME = "openneuro-datasets"
    DATASET_PREFIX = "processed/ds005691"  # Replace with your dataset ID

    try:
        # Load results
        print("Loading results from S3...")
        results = load_results_from_s3(BUCKET_NAME, DATASET_PREFIX)

        # Verify and visualize
        print("\nVerifying results and creating plots...")
        verify_and_visualize_results(results)

        print("\nVerification complete!")

    except Exception as e:
        print(f"Error during verification: {str(e)}")


if __name__ == "__main__":
    main()