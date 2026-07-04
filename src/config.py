"""Carrega a configuracao central do pipeline a partir do config.yaml."""

from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Le o config.yaml e retorna a configuracao como dicionario.

    :param path: caminho do arquivo de configuracao (default: src/config.yaml).
    :return: dicionario com a configuracao completa do pipeline.
    """
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def table_name(config: dict, layer: str, table: str) -> str:
    """Monta o nome completo (catalogo.schema.tabela) de uma tabela.

    :param config: configuracao carregada via load_config().
    :param layer: camada logica ('bronze', 'silver' ou 'gold').
    :param table: nome da tabela dentro da camada.
    :return: nome qualificado no padrao Unity Catalog.
    """
    return f"{config['catalog']}.{config['schemas'][layer]}.{table}"
