from dataclasses import dataclass

@dataclass
class CodexType:
    codex: str
    name: str
    rarity: str
    icon: str
    description: str
    meta: list
    stat: list
    drop: dict