"""The 27 PhantomLedger mule_temporal tables, in endpoint-safe load order."""

from dataclasses import dataclass
from pathlib import Path

GRAPH = "Mule_Pattern_Learner"
SOURCE_SCHEMA = "mule_temporal"
FORMAT_VERSION = 4
ROOT = Path(__file__).resolve().parents[3]
GSQL = ROOT / "gsql" / "mule_temporal"


@dataclass(frozen=True)
class Dataset:
    name: str
    fields: tuple[tuple[str, str], ...]
    source: str = ""
    target: str = ""
    reverse: str = ""

    @property
    def association(self) -> bool:
        return "valid_from_seq" in self.columns

    @property
    def vertex(self) -> bool:
        return not self.source

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.fields)

    @property
    def graph_fields(self) -> tuple[tuple[str, str], ...]:
        """Storage order includes graph attributes absent from the source feed."""
        if self.name == "Account":
            return self.fields[:-1] + fields(
                "mule_label_known:BOOL is_mule_masked:BOOL pu_label:INT "
                "mule_label_effective_seq:UINT mule_label_effective_ts_ms:UINT "
                "mule_label_available_seq:UINT mule_label_available_ts_ms:UINT "
                "mule_ring_id:INT mule_label_source:STRING is_mule:INT"
            )
        if self.name in ("Payment_Transaction", "Zelle_Transfer"):
            return self.fields + fields(
                "pair_delta_t_ms:UINT pair_delta_t_present:BOOL pair_time_encoding:LIST<DOUBLE> "
                "time_encoding_basis_id:STRING pair_previous_event_id:STRING "
                "pair_sender_id:STRING pair_recipient_type:STRING pair_recipient_id:STRING"
            )
        return self.fields

    @property
    def job(self) -> str:
        return "mt_load_" + self.name.lower()

    @property
    def table(self) -> str:
        return f'{SOURCE_SCHEMA}."mt_{self.name}"'

    @property
    def view(self) -> str:
        return f'pg_temp."mt_{self.name}"'


def fields(value: str) -> tuple[tuple[str, str], ...]:
    return tuple((part.split(":")[0], part.split(":")[1]) for part in value.split())


VERTICES = (
    Dataset(
        "Party",
        fields("id:STRING party_type:STRING first_seen_seq:UINT first_seen_ts_ms:UINT"),
    ),
    Dataset(
        "Account",
        fields(
            "id:STRING account_type:STRING is_external:BOOL first_seen_seq:UINT first_seen_ts_ms:UINT is_mule:INT"
        ),
    ),
    Dataset(
        "Token",
        fields(
            "token_id:STRING first_seen_seq:UINT first_seen_ts_ms:UINT token_kind:STRING token_network:STRING"
        ),
    ),
    Dataset(
        "Device",
        fields(
            "id:STRING device_type:STRING first_seen_seq:UINT first_seen_ts_ms:UINT"
        ),
    ),
    Dataset("IP", fields("id:STRING first_seen_seq:UINT first_seen_ts_ms:UINT")),
    Dataset(
        "Address",
        fields(
            "address_id:STRING first_seen_seq:UINT first_seen_ts_ms:UINT country_code:STRING"
        ),
    ),
    Dataset(
        "Payment_Transaction",
        fields(
            "transaction_id:STRING amount:DOUBLE currency:STRING payment_rail:STRING channel:STRING event_time:DATETIME event_ts_ms:UINT event_seq:UINT amount_present:BOOL"
        ),
    ),
    Dataset(
        "Zelle_Transfer",
        fields(
            "transfer_id:STRING event_time:DATETIME event_ts_ms:UINT event_seq:UINT amount:DOUBLE amount_present:BOOL currency:STRING channel:STRING fraud_label:INT label_known:BOOL label_available_seq:UINT label_available_ts_ms:UINT"
        ),
    ),
)
ASSOCIATION_FIELDS = fields(
    "from_id:STRING to_id:STRING valid_from_seq:UINT valid_to_seq:UINT confidence:FLOAT source_system:STRING"
)
PARTICIPATION_FIELDS = fields(
    "from_id:STRING to_id:STRING event_ts_ms:UINT event_seq:UINT"
)
ASSOCIATIONS = tuple(
    Dataset(name, ASSOCIATION_FIELDS, source, target, reverse)
    for name, source, target, reverse in (
        ("Party_Owns_Account", "Party", "Account", "Account_Owned_By_Party"),
        ("Party_Uses_Token", "Party", "Token", "Token_Used_By_Party"),
        ("Token_Bound_To_Account", "Token", "Account", "Account_Bound_From_Token"),
        ("Party_Uses_Device", "Party", "Device", "Device_Used_By_Party"),
        ("Account_Uses_Device", "Account", "Device", "Device_Used_By_Account"),
        ("Party_Uses_IP", "Party", "IP", "IP_Used_By_Party"),
        ("Party_Has_Address", "Party", "Address", "Address_Used_By_Party"),
    )
)
PARTICIPATIONS = tuple(
    Dataset(name, PARTICIPATION_FIELDS, source, target, reverse)
    for name, source, target, reverse in (
        (
            "Transfer_From_Account",
            "Zelle_Transfer",
            "Account",
            "Account_Sent_Zelle_Transfer",
        ),
        (
            "Transfer_To_Account",
            "Zelle_Transfer",
            "Account",
            "Account_Received_Zelle_Transfer",
        ),
        ("Transfer_From_Token", "Zelle_Transfer", "Token", "Token_Sent_Transfer"),
        ("Transfer_To_Token", "Zelle_Transfer", "Token", "Token_Received_Transfer"),
        ("Transfer_Used_Device", "Zelle_Transfer", "Device", "Device_Used_By_Transfer"),
        ("Transfer_Used_IP", "Zelle_Transfer", "IP", "IP_Used_By_Transfer"),
        (
            "Transaction_From_Account",
            "Payment_Transaction",
            "Account",
            "Account_Initiated_Transaction",
        ),
        (
            "Transaction_To_Account",
            "Payment_Transaction",
            "Account",
            "Account_Received_Transaction",
        ),
        (
            "Transaction_From_Token",
            "Payment_Transaction",
            "Token",
            "Token_Sent_Transaction",
        ),
        (
            "Transaction_To_Token",
            "Payment_Transaction",
            "Token",
            "Token_Received_Transaction",
        ),
        (
            "Transaction_Used_Device",
            "Payment_Transaction",
            "Device",
            "Device_Used_By_Transaction",
        ),
        (
            "Transaction_Used_IP",
            "Payment_Transaction",
            "IP",
            "IP_Observed_In_Transaction",
        ),
    )
)
DATASETS = VERTICES + ASSOCIATIONS + PARTICIPATIONS
BY_NAME = {d.name: d for d in DATASETS}


def visible(start: int, end: int, seed: int) -> bool:
    """Half-open valid-time predicate, identical in both edge directions."""
    return start <= seed and (end == 0 or seed < end)
