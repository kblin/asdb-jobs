# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""Shared BLAST logic"""

from dataclasses import asdict, dataclass, fields
from typing import Any

from aiostandalone import StandaloneApplication

@dataclass
class BlastResult:
    q_acc: str
    s_acc: str
    identity: float
    q_seq: str
    q_start: int
    q_end: int
    q_len: int
    s_seq: str
    s_start: int
    s_end: int
    s_len: int


    @classmethod
    def from_line(cls, line: str) -> "BlastResult":
        """Parse from a line of blastp results"""
        parts = line.strip().split("\t")
        if len(parts) != len(fields(cls)):
            raise ValueError(f"Unexpected line {parts}")

        q_acc = parts[0]
        s_acc = parts[1]
        nident = int(parts[2])
        q_seq = parts[3]
        q_start = int(parts[4])
        q_end = int(parts[5])
        q_len = int(parts[6])
        identity = round((nident / q_len) * 100)
        s_seq = parts[7]
        s_start = int(parts[8])
        s_end = int(parts[9])
        s_len = int(parts[10])

        return cls(
            q_acc, s_acc, identity,
            q_seq, q_start, q_end, q_len,
            s_seq, s_start, s_end, s_len,
        )
    
    def to_json(self) -> dict[str, Any]:
        """Convert to a JSON-able datastructure"""
        return asdict(self)


def parse_blast(lines: list[str]) -> list[BlastResult]:
    """Parse blast results"""
    res: list[BlastResult] = []
    for line in lines:
        res.append(BlastResult.from_line(line))

    return res


@dataclass
class ComparippsonResult:
    q_acc: str
    s_locus: str
    s_type: str
    s_acc: str
    s_rec_start: str
    s_rec_end: str
    identity: float
    q_seq: str
    q_start: int
    q_end: int
    q_len: int
    s_seq: str
    s_start: int
    s_end: int
    s_len: int

    @classmethod
    def from_blast(cls, blast: BlastResult, metadata: dict[str, Any]) -> "ComparippsonResult":
        """Load from a BlastResult"""
        entry_id = blast.s_acc.split("|")[0]
        data = metadata["entries"][entry_id]
        s_locus = data["locus"]
        s_type = data["type"]
        s_acc = data["accession"]
        s_rec_start = data["start"]
        s_rec_end = data["end"]

        return cls(
            blast.q_acc,
            s_locus, s_type, s_acc, s_rec_start, s_rec_end,
            blast.identity,
            blast.q_seq, blast.q_start, blast.q_end, blast.q_len,
            blast.s_seq, blast.s_start, blast.s_end, blast.s_len,
        )

    def to_json(self) -> dict[str, Any]:
        """Convert to a JSON-able datastructure"""
        return asdict(self)