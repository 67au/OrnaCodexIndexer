from dataclasses import dataclass

@dataclass
class CodexType:
    codex: str
    name: str
    rarity: str
    icon: str
    description: list
    meta: list
    tag: list
    stat: list
    drop: dict