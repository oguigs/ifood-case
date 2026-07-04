"""Job de extracao: baixa os parquets da NYC TLC para a landing zone.

A landing zone e um Unity Catalog Volume, montado via FUSE no caminho
/Volumes/..., o que permite manipula-lo com a biblioteca padrao do Python.
"""

import os
import time

import requests


class ExtractJob:
    """Baixa os arquivos mensais de yellow e green taxis para a landing zone."""

    def __init__(self, config: dict):
        self.base_url = config["source"]["base_url"]
        self.year = config["source"]["year"]
        self.months = config["source"]["months"]
        self.taxi_types = list(config["taxi_types"].keys())
        self.landing_volume = config["landing_volume"]
        self.retries = config["download"]["retries"]
        self.backoff_seconds = config["download"]["backoff_seconds"]
        self.chunk_size = config["download"]["chunk_size_bytes"]
        self.timeout = config["download"]["timeout_seconds"]

    def run(self) -> None:
        """Executa o download de todos os arquivos do periodo configurado."""
        downloaded, skipped, failed = 0, 0, []
        for taxi_type in self.taxi_types:
            target_dir = os.path.join(self.landing_volume, taxi_type)
            os.makedirs(target_dir, exist_ok=True)
            for month in self.months:
                file_name = f"{taxi_type}_tripdata_{self.year}-{month:02d}.parquet"
                destination = os.path.join(target_dir, file_name)
                if os.path.exists(destination):
                    print(f"[skip] {file_name} ja existe na landing zone")
                    skipped += 1
                    continue
                if self._download_with_retry(file_name, destination):
                    downloaded += 1
                else:
                    failed.append(file_name)

        print(
            f"\nExtracao concluida: {downloaded} baixados, "
            f"{skipped} ja existentes, {len(failed)} falhas."
        )
        if failed:
            raise RuntimeError(f"Falha no download dos arquivos: {failed}")

    def _download_with_retry(self, file_name: str, destination: str) -> bool:
        """Baixa um arquivo com retry + backoff, escrevendo em chunks."""
        url = f"{self.base_url}/{file_name}"
        for attempt in range(1, self.retries + 1):
            try:
                print(f"[get ] {file_name} (tentativa {attempt}/{self.retries})")
                response = requests.get(url, stream=True, timeout=self.timeout)
                response.raise_for_status()
                temp_path = destination + ".tmp"
                with open(temp_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        file.write(chunk)
                os.rename(temp_path, destination)
                size_mb = os.path.getsize(destination) / 1e6
                print(f"[ok  ] {file_name} ({size_mb:.1f} MB)")
                return True
            except requests.exceptions.RequestException as error:
                print(f"[err ] {file_name}: {error}")
                if attempt < self.retries:
                    time.sleep(self.backoff_seconds * attempt)
        return False
