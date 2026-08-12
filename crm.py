import json
import os
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import (
    and_,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    case,
    create_engine,
    delete,
    func,
    or_,
    select,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


CRM_STATUSES = (
    'new',
    'research_needed',
    'contact_ready',
    'attempted_contact',
    'interested',
    'appointment_scheduled',
    'negotiation',
    'contract_pending',
    'closed',
    'disqualified',
)

CRM_PRIORITIES = ('urgent', 'high', 'medium', 'normal')
PIPELINE_STAGES = (
    'new',
    'research_needed',
    'contact_ready',
    'attempted_contact',
    'interested',
    'appointment_scheduled',
    'negotiation',
    'contract_pending',
    'closed',
)
RESEARCH_STATUSES = ('unreviewed', 'in_progress', 'verified', 'rejected')
EVIDENCE_TYPES = (
    'probate_case',
    'death_index',
    'death_certificate',
    'obituary',
    'executor_appointment',
    'heir_or_relative',
    'assessor_owner_change',
    'estate_text',
    'skip_trace_mismatch',
    'other',
)
EVIDENCE_OUTCOMES = ('supports_deceased', 'supports_living', 'inconclusive')
EVIDENCE_CONFIDENCE = ('confirmed', 'strong', 'weak', 'rejected')
IDENTITY_MATCHES = ('exact', 'probable', 'uncertain', 'mismatch')
PROBATE_CONTACT_ROLES = (
    'executor',
    'personal_representative',
    'administrator',
    'heir',
    'relative',
    'attorney',
    'other',
)
CONTACT_KINDS = ('phone', 'email')
CONTACT_CONFIDENCE = ('unverified', 'probable', 'verified')
CONTACT_STATUSES = ('active', 'invalid', 'do_not_contact')
CAMPAIGN_STATUSES = ('active', 'completed', 'archived')
SKIP_TRACE_SELECTIONS = ('high_priority', 'potentially_deceased', 'manual')
CALL_OUTCOMES = (
    'no_answer',
    'voicemail_left',
    'wrong_number',
    'call_later',
    'spoke_follow_up',
    'not_interested',
    'appointment_set',
    'offer_requested',
    'deal_pending',
)
CALL_DIRECTIONS = ('outbound', 'inbound')
CALL_OUTCOME_STATUSES = {
    'no_answer': 'attempted_contact',
    'voicemail_left': 'attempted_contact',
    'wrong_number': 'contact_ready',
    'call_later': 'attempted_contact',
    'spoke_follow_up': 'interested',
    'not_interested': 'disqualified',
    'appointment_set': 'appointment_scheduled',
    'offer_requested': 'negotiation',
    'deal_pending': 'contract_pending',
}
CALL_OUTCOMES_REQUIRING_FOLLOW_UP = (
    'no_answer',
    'voicemail_left',
    'call_later',
    'spoke_follow_up',
    'appointment_set',
    'offer_requested',
    'deal_pending',
)

RESEARCH_CATEGORIES = (
    'deceased_estate',
    'ownership_mismatch',
    'care_of_representative',
    'out_of_state',
    'po_box',
    'mailing_city_mismatch',
    'other_mailing',
)
RESEARCH_CATEGORY_LABELS = {
    'deceased_estate': 'Deceased & Estate',
    'ownership_mismatch': 'Ownership mismatch',
    'care_of_representative': 'Care of / representative',
    'out_of_state': 'Out-of-state',
    'po_box': 'PO Box',
    'mailing_city_mismatch': 'Different mailing city',
    'other_mailing': 'Other mailing anomaly',
}
RESEARCH_CATEGORY_DESCRIPTIONS = {
    'deceased_estate': 'Estate text or another direct deceased-owner signal',
    'ownership_mismatch': 'County owner and researched identity may differ',
    'care_of_representative': 'Mail is routed through a representative or third party',
    'out_of_state': 'Owner mailing address is outside Oklahoma',
    'po_box': 'Owner receives correspondence through a post-office box',
    'mailing_city_mismatch': 'Mailing city differs from the property municipality',
    'other_mailing': 'Another mailing pattern requires human review',
}
PROBATE_STAGE_LABELS = {
    'direct_signal': 'Direct estate signal',
    'representative_signal': 'Representative candidate',
    'evidence_review': 'Evidence in review',
    'needs_resolution': 'Conflict to resolve',
    'probable': 'Probable deceased',
    'confirmed': 'Confirmed deceased',
    'false_positive': 'False positive',
}


def utc_now():
    return datetime.now(timezone.utc)


def research_queue_condition():
    """Canonical definition shared by queue and dashboard metrics."""
    return and_(
        Lead.status == 'research_needed',
        Lead.research_status.in_(('unreviewed', 'in_progress')),
    )


def research_reason(lead):
    reasons = []
    if lead.deceased_flag:
        reasons.append('Deceased-owner signal')
    mailing_signal = str(lead.mailing_signal or '').strip()
    if mailing_signal:
        reasons.append(f'Mailing signal: {mailing_signal}')
    return ' · '.join(reasons) or 'No active research signal'


def research_category(lead):
    if lead.deceased_flag:
        return 'deceased_estate'
    signal = str(lead.mailing_signal or '').strip().lower()
    if any(term in signal for term in (
        'owner mismatch',
        'ownership mismatch',
        'identity mismatch',
        'assessor mismatch',
    )):
        return 'ownership_mismatch'
    if any(term in signal for term in (
        'care of',
        'c/o',
        'representative',
        'executor',
    )):
        return 'care_of_representative'
    if 'out of state' in signal or 'out-of-state' in signal:
        return 'out_of_state'
    if 'po box' in signal or 'p.o. box' in signal:
        return 'po_box'
    if 'different mailing city' in signal or 'mailing city mismatch' in signal:
        return 'mailing_city_mismatch'
    return 'other_mailing'


def research_category_payload(lead):
    category = research_category(lead)
    return {
        'research_category': category,
        'research_category_label': RESEARCH_CATEGORY_LABELS[category],
        'research_category_description': (
            RESEARCH_CATEGORY_DESCRIPTIONS[category]
        ),
    }


def _lead_source_payload(lead):
    try:
        payload = json.loads(lead.source_data_json or '{}')
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _clean_source_identifier(value):
    if value is None:
        return ''
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def normalize_contact_value(kind, value):
    value = str(value or '').strip()
    if kind == 'email':
        return value.lower()
    digits = re.sub(r'\D', '', value)
    return digits[-10:] if len(digits) >= 10 else digits


def _lead_property_identity(lead):
    payload = _lead_source_payload(lead)
    explicit = str(payload.get('Property Lead Key') or '').strip()
    if explicit:
        prefix, separator, identifier = explicit.partition(':')
        if separator and prefix.lower() == 'pid':
            return f'pid:{identifier.strip().upper()}'
        return explicit.lower()
    pid = _clean_source_identifier(payload.get('PID'))
    normalized_pid = ''.join(character for character in pid if character.isalnum())
    if normalized_pid:
        if not normalized_pid.upper().startswith('R'):
            normalized_pid = f'R{normalized_pid}'
        return f'pid:{normalized_pid.upper()}'
    return (
        f"address:{(lead.property_address or '').strip().upper()}|"
        f"{(lead.property_city or '').strip().upper()}"
    )


def _lead_assessor_account_no(lead):
    identity = _lead_property_identity(lead)
    if identity.startswith('pid:'):
        return identity.split(':', 1)[1].strip().upper()
    return ''


def _lead_property_context(lead):
    payload = _lead_source_payload(lead)
    return {
        'property_key': _lead_property_identity(lead),
        'parcel_id': _clean_source_identifier(payload.get('PID')),
        'tax_ids': str(payload.get('Tax IDs') or lead.tax_id or '').strip(),
        'current_owner_verification': str(
            payload.get('Current Owner Verification') or ''
        ).strip(),
        'current_assessor_owner': str(
            payload.get('Current Assessor Owner') or ''
        ).strip(),
        'assessor_url': str(payload.get('Assessor URL') or '').strip(),
    }


def normalize_database_url(database_target):
    """Accept a local file path or a Render-style PostgreSQL URL."""
    target = str(database_target)
    if '://' not in target:
        path = os.path.abspath(target)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return f"sqlite:///{path}"
    if target.startswith('postgres://'):
        return target.replace('postgres://', 'postgresql+psycopg://', 1)
    if target.startswith('postgresql://'):
        return target.replace('postgresql://', 'postgresql+psycopg://', 1)
    return target


class Base(DeclarativeBase):
    pass


class ImportRun(Base):
    __tablename__ = 'import_runs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    source_filename: Mapped[str] = mapped_column(Text, nullable=False)
    output_filename: Mapped[str] = mapped_column(Text, nullable=False)
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False)
    stats_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Campaign(Base):
    __tablename__ = 'campaigns'
    __table_args__ = (Index('idx_campaign_status', 'status'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    source_job_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default='active')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ProcessingJob(Base):
    __tablename__ = 'processing_jobs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    meta_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ProcessingArtifact(Base):
    __tablename__ = 'processing_artifacts'
    __table_args__ = (
        UniqueConstraint('job_id', 'kind', name='uq_job_artifact_kind'),
        Index('idx_artifact_filename', 'filename'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey('processing_jobs.job_id', ondelete='CASCADE'),
        nullable=False,
    )
    kind: Mapped[str] = mapped_column(String(40), nullable=False)
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Lead(Base):
    __tablename__ = 'leads'
    __table_args__ = (
        UniqueConstraint(
            'source_job_id',
            'tax_id',
            'owner_name',
            'property_address',
            name='uq_lead_source_identity',
        ),
        Index('idx_leads_status', 'status'),
        Index('idx_leads_priority', 'priority'),
        Index('idx_leads_research', 'research_status'),
        Index('idx_leads_owner', 'owner_name'),
        Index('idx_leads_tax_id', 'tax_id'),
        Index('idx_leads_source_job', 'source_job_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tax_id: Mapped[str | None] = mapped_column(Text)
    owner_name: Mapped[str] = mapped_column(Text, nullable=False)
    property_address: Mapped[str | None] = mapped_column(Text)
    property_city: Mapped[str | None] = mapped_column(Text)
    mailing_address: Mapped[str | None] = mapped_column(Text)
    mailing_city: Mapped[str | None] = mapped_column(Text)
    mailing_state: Mapped[str | None] = mapped_column(Text)
    zip_code: Mapped[str | None] = mapped_column(Text)
    total_due: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False)
    deceased_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    mailing_signal: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(40), nullable=False, default='new')
    priority: Mapped[str] = mapped_column(String(20), nullable=False, default='normal')
    research_status: Mapped[str] = mapped_column(
        String(30), nullable=False, default='unreviewed'
    )
    phone: Mapped[str | None] = mapped_column(Text)
    email: Mapped[str | None] = mapped_column(Text)
    next_follow_up: Mapped[str | None] = mapped_column(String(30))
    last_contacted_at: Mapped[str | None] = mapped_column(String(40))
    source_job_id: Mapped[str] = mapped_column(
        String(100), ForeignKey('import_runs.job_id'), nullable=False
    )
    source_data_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class LeadNote(Base):
    __tablename__ = 'lead_notes'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(
        ForeignKey('leads.id', ondelete='CASCADE'), nullable=False
    )
    body: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class LeadActivity(Base):
    __tablename__ = 'lead_activity'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(
        ForeignKey('leads.id', ondelete='CASCADE'), nullable=False
    )
    activity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    detail: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CallLog(Base):
    __tablename__ = 'call_logs'
    __table_args__ = (
        Index('idx_call_logs_lead', 'lead_id'),
        Index('idx_call_logs_outcome', 'outcome'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(
        ForeignKey('leads.id', ondelete='CASCADE'), nullable=False
    )
    direction: Mapped[str] = mapped_column(String(20), nullable=False)
    outcome: Mapped[str] = mapped_column(String(40), nullable=False)
    phone_number: Mapped[str | None] = mapped_column(Text)
    duration_minutes: Mapped[int | None] = mapped_column(Integer)
    notes: Mapped[str | None] = mapped_column(Text)
    next_follow_up: Mapped[str | None] = mapped_column(String(30))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ResearchEvidence(Base):
    __tablename__ = 'research_evidence'
    __table_args__ = (
        Index('idx_evidence_lead', 'lead_id'),
        Index('idx_evidence_outcome', 'outcome'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(
        ForeignKey('leads.id', ondelete='CASCADE'), nullable=False
    )
    evidence_type: Mapped[str] = mapped_column(String(50), nullable=False)
    outcome: Mapped[str] = mapped_column(String(40), nullable=False)
    confidence: Mapped[str] = mapped_column(String(30), nullable=False)
    identity_match: Mapped[str] = mapped_column(String(30), nullable=False)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text)
    case_number: Mapped[str | None] = mapped_column(String(100))
    subject_name: Mapped[str | None] = mapped_column(Text)
    event_date: Mapped[str | None] = mapped_column(String(30))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    retracted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retraction_reason: Mapped[str | None] = mapped_column(Text)


class ProbateContact(Base):
    __tablename__ = 'probate_contacts'
    __table_args__ = (
        Index('idx_probate_contacts_lead', 'lead_id'),
        Index('idx_probate_contacts_role', 'role'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(
        ForeignKey('leads.id', ondelete='CASCADE'), nullable=False
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(String(40), nullable=False)
    phone: Mapped[str | None] = mapped_column(Text)
    email: Mapped[str | None] = mapped_column(Text)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class ContactPoint(Base):
    __tablename__ = 'contact_points'
    __table_args__ = (
        Index('idx_contact_points_lead', 'lead_id'),
        Index('idx_contact_points_status', 'status'),
        Index(
            'idx_contact_points_identity',
            'lead_id',
            'kind',
            'normalized_value',
            unique=True,
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(
        ForeignKey('leads.id', ondelete='CASCADE'), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_value: Mapped[str] = mapped_column(Text, nullable=False)
    label: Mapped[str | None] = mapped_column(String(60))
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class AssessorVerification(Base):
    __tablename__ = 'assessor_verifications'
    __table_args__ = (Index('idx_assessor_status', 'status'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_no: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(String(40), nullable=False)
    current_owner: Mapped[str | None] = mapped_column(Text)
    account_type: Mapped[str | None] = mapped_column(String(80))
    vacant: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class EnrichmentBatch(Base):
    __tablename__ = 'enrichment_batches'
    __table_args__ = (Index('idx_enrichment_status', 'status'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    lead_ids_json: Mapped[str] = mapped_column(Text, nullable=False)
    cost_per_record: Mapped[float] = mapped_column(Float, nullable=False)
    budget_cap: Mapped[float] = mapped_column(Float, nullable=False)
    estimated_cost: Mapped[float] = mapped_column(Float, nullable=False)
    result_summary_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    campaign_id: Mapped[int | None] = mapped_column(ForeignKey('campaigns.id'))
    selection_mode: Mapped[str | None] = mapped_column(String(30))
    actual_cost: Mapped[float | None] = mapped_column(Float)
    error_json: Mapped[str | None] = mapped_column(Text)


class DailyCheckIn(Base):
    __tablename__ = 'daily_check_ins'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    work_date: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    focus: Mapped[str] = mapped_column(Text, nullable=False)
    call_target: Mapped[int] = mapped_column(Integer, nullable=False)
    research_target: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    closing_notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OperationalAlert(Base):
    __tablename__ = 'operational_alerts'
    __table_args__ = (
        Index('idx_alert_read', 'read_at'),
        Index('idx_alert_severity', 'severity'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(180), unique=True, nullable=False)
    alert_type: Mapped[str] = mapped_column(String(40), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    detail: Mapped[str] = mapped_column(Text, nullable=False)
    href: Mapped[str] = mapped_column(Text, nullable=False)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    emailed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    sms_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


def _as_dict(instance):
    return {
        column.name: getattr(instance, column.name)
        for column in instance.__table__.columns
    }


def summarize_evidence(items):
    items = [item for item in items if not item.retracted_at]
    if not items:
        return {
            'status': 'no_evidence',
            'label': 'No evidence',
            'confirmed': False,
            'reason': 'No research evidence has been recorded',
        }
    living = [
        item for item in items
        if item.outcome == 'supports_living'
        and item.confidence == 'confirmed'
        and item.identity_match in ('exact', 'probable')
    ]
    official_death_types = {
        'probate_case',
        'death_index',
        'death_certificate',
        'executor_appointment',
    }
    confirmed = [
        item for item in items
        if item.outcome == 'supports_deceased'
        and item.confidence == 'confirmed'
        and item.identity_match == 'exact'
        and item.evidence_type in official_death_types
    ]
    if living and confirmed:
        return {
            'status': 'conflicting_evidence',
            'label': 'Conflicting evidence',
            'confirmed': False,
            'reason': 'Confirmed living and death evidence require manual resolution',
        }
    if living:
        return {
            'status': 'false_positive',
            'label': 'False positive',
            'confirmed': False,
            'reason': 'Confirmed evidence supports that the matched person is living',
        }
    if confirmed:
        return {
            'status': 'confirmed_deceased',
            'label': 'Confirmed deceased',
            'confirmed': True,
            'reason': 'Exact identity match supported by confirmed official evidence',
        }
    strong = [
        item for item in items
        if item.outcome == 'supports_deceased'
        and item.confidence in ('confirmed', 'strong')
        and item.identity_match in ('exact', 'probable')
    ]
    if strong:
        return {
            'status': 'probable_deceased',
            'label': 'Probable deceased',
            'confirmed': False,
            'reason': 'Strong evidence exists but confirmation requirements are incomplete',
        }
    return {
        'status': 'inconclusive',
        'label': 'Inconclusive',
        'confirmed': False,
        'reason': 'Recorded evidence does not establish identity and death',
    }


def probate_case_stage(lead, evidence_summary):
    status = evidence_summary['status']
    if status == 'confirmed_deceased':
        return 'confirmed'
    if status == 'probable_deceased':
        return 'probable'
    if status == 'false_positive':
        return 'false_positive'
    if status == 'conflicting_evidence':
        return 'needs_resolution'
    if status == 'inconclusive':
        return 'evidence_review'
    if lead.deceased_flag:
        return 'direct_signal'
    return 'representative_signal'


class CRMRepository:
    def __init__(self, database_target):
        self.database_url = normalize_database_url(database_target)
        connect_args = (
            {'check_same_thread': False}
            if self.database_url.startswith('sqlite:')
            else {}
        )
        self.engine = create_engine(
            self.database_url,
            pool_pre_ping=True,
            connect_args=connect_args,
        )
        self.Session = sessionmaker(self.engine, expire_on_commit=False)
        self._initialize_lock = threading.Lock()
        self._initialized = False

    def initialize(self):
        if self._initialized:
            return
        with self._initialize_lock:
            if self._initialized:
                return
            Base.metadata.create_all(self.engine)
            self._initialized = True

    def health(self):
        with self.engine.connect() as connection:
            connection.execute(select(1))
        return {'status': 'ok', 'database': self.engine.dialect.name}

    def save_processing_job(self, meta):
        now = utc_now()
        payload = json.dumps(meta, default=str)
        with self.Session.begin() as session:
            item = session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.job_id == meta['uid']
                )
            )
            if item:
                item.meta_json = payload
                item.updated_at = now
            else:
                session.add(
                    ProcessingJob(
                        job_id=meta['uid'],
                        meta_json=payload,
                        created_at=now,
                        updated_at=now,
                    )
                )

    def get_processing_job(self, job_id):
        with self.Session() as session:
            item = session.scalar(
                select(ProcessingJob).where(ProcessingJob.job_id == job_id)
            )
            if not item:
                return None
            meta = json.loads(item.meta_json)
            meta['_created_at'] = item.created_at.isoformat()
            meta['_updated_at'] = item.updated_at.isoformat()
            return meta

    def list_processing_jobs(self, limit=20):
        with self.Session() as session:
            items = session.scalars(
                select(ProcessingJob)
                .order_by(ProcessingJob.updated_at.desc())
                .limit(min(max(int(limit), 1), 100))
            ).all()
            result = []
            for item in items:
                meta = json.loads(item.meta_json)
                meta['_created_at'] = item.created_at.isoformat()
                meta['_updated_at'] = item.updated_at.isoformat()
                result.append(meta)
            return result

    def save_processing_artifact(self, job_id, kind, filename, content):
        import hashlib

        now = utc_now()
        content = bytes(content)
        digest = hashlib.sha256(content).hexdigest()
        with self.Session.begin() as session:
            item = session.scalar(
                select(ProcessingArtifact).where(
                    ProcessingArtifact.job_id == job_id,
                    ProcessingArtifact.kind == kind,
                )
            )
            values = {
                'filename': filename,
                'content': content,
                'content_sha256': digest,
                'size_bytes': len(content),
                'updated_at': now,
            }
            if item:
                for field, value in values.items():
                    setattr(item, field, value)
            else:
                item = ProcessingArtifact(
                    job_id=job_id,
                    kind=kind,
                    created_at=now,
                    **values,
                )
                session.add(item)
        return {
            'job_id': job_id,
            'kind': kind,
            'filename': filename,
            'content_sha256': digest,
            'size_bytes': len(content),
        }

    def get_processing_artifact(self, job_id=None, kind=None, filename=None):
        conditions = []
        if job_id:
            conditions.append(ProcessingArtifact.job_id == job_id)
        if kind:
            conditions.append(ProcessingArtifact.kind == kind)
        if filename:
            conditions.append(ProcessingArtifact.filename == filename)
        if not conditions:
            return None
        with self.Session() as session:
            item = session.scalar(
                select(ProcessingArtifact)
                .where(*conditions)
                .order_by(ProcessingArtifact.updated_at.desc())
            )
            return _as_dict(item) if item else None

    def get_assessor_verification(self, account_no):
        with self.Session() as session:
            item = session.scalar(
                select(AssessorVerification).where(
                    AssessorVerification.account_no == account_no
                )
            )
            return _as_dict(item) if item else None

    def save_assessor_verification(self, result):
        fetched_at = datetime.fromisoformat(result.fetched_at)
        with self.Session.begin() as session:
            item = session.scalar(
                select(AssessorVerification).where(
                    AssessorVerification.account_no == result.account_no
                )
            )
            values = {
                'status': result.status,
                'current_owner': result.current_owner,
                'account_type': result.account_type,
                'vacant': result.vacant,
                'source_url': result.source_url,
                'error': result.error,
                'fetched_at': fetched_at,
            }
            if item:
                for field, value in values.items():
                    setattr(item, field, value)
            else:
                item = AssessorVerification(
                    account_no=result.account_no,
                    **values,
                )
                session.add(item)
            session.flush()
            record = _as_dict(item)
        return record

    @staticmethod
    def _value(row, column, default=''):
        if not column:
            return default
        value = row.get(column, default)
        if pd.isna(value):
            return default
        return value

    @staticmethod
    def _record_json(row):
        record = {}
        for key, value in row.items():
            if pd.isna(value):
                record[str(key)] = None
            elif hasattr(value, 'item'):
                record[str(key)] = value.item()
            else:
                record[str(key)] = value
        return json.dumps(record, default=str)

    @staticmethod
    def _priority(deceased_flag, mailing_signal, total_due):
        if deceased_flag or mailing_signal == 'Strong' or total_due >= 10000:
            return 'high'
        if mailing_signal or total_due >= 5000:
            return 'medium'
        return 'normal'

    def import_leads(self, dataframe, job, columns):
        now = utc_now()
        imported = 0
        with self.Session.begin() as session:
            campaign = session.scalar(
                select(Campaign).where(Campaign.source_job_id == job['uid'])
            )
            if not campaign:
                campaign = Campaign(
                    name=str(job.get('campaign_name') or (
                        f"{os.path.splitext(job['source_filename'])[0]} · "
                        f"{job['tax_year']}"
                    )).strip(),
                    source_job_id=job['uid'], status='active',
                    created_at=now, updated_at=now,
                )
                session.add(campaign)
            import_run = session.scalar(
                select(ImportRun).where(ImportRun.job_id == job['uid'])
            )
            if import_run:
                import_run.output_filename = job['output_filename']
                import_run.stats_json = json.dumps(job['stats'])
            else:
                session.add(
                    ImportRun(
                        job_id=job['uid'],
                        source_filename=job['source_filename'],
                        output_filename=job['output_filename'],
                        tax_year=job['tax_year'],
                        stats_json=json.dumps(job['stats']),
                        created_at=now,
                    )
                )
                session.flush()

            for _, row in dataframe.iterrows():
                owner_name = str(self._value(row, columns.get('owner_name'))).strip()
                if not owner_name:
                    continue
                property_address = ' '.join(
                    str(part).strip()
                    for part in (
                        self._value(row, columns.get('street_number')),
                        self._value(row, columns.get('street_name')),
                        self._value(row, columns.get('street_type')),
                    )
                    if str(part).strip()
                )
                tax_id = str(self._value(row, columns.get('tax_id'))).strip()
                duplicate = session.scalar(
                    select(Lead.id).where(
                        Lead.source_job_id == job['uid'],
                        Lead.tax_id == tax_id,
                        Lead.owner_name == owner_name,
                        Lead.property_address == property_address,
                    )
                )
                if duplicate:
                    continue

                try:
                    total_due = float(
                        self._value(row, columns.get('total_due'), 0)
                    )
                except (TypeError, ValueError):
                    total_due = 0
                deceased_value = str(
                    self._value(row, columns.get('deceased_flag'))
                ).strip()
                deceased_flag = (
                    bool(deceased_value)
                    and deceased_value.upper()
                    not in ('NO', 'FALSE', 'NONE', 'NAN')
                )
                mailing_signal = str(
                    self._value(row, columns.get('mailing_signal'))
                ).strip()
                lead = Lead(
                    tax_id=tax_id,
                    owner_name=owner_name,
                    property_address=property_address,
                    property_city=str(
                        self._value(row, columns.get('property_city'))
                    ).strip(),
                    mailing_address=str(
                        self._value(row, columns.get('mailing_address'))
                    ).strip(),
                    mailing_city=str(
                        self._value(row, columns.get('mailing_city'))
                    ).strip(),
                    mailing_state=str(
                        self._value(row, columns.get('mailing_state'))
                    ).strip(),
                    zip_code=str(
                        self._value(row, columns.get('zip_code'))
                    ).strip(),
                    total_due=total_due,
                    tax_year=job['tax_year'],
                    deceased_flag=deceased_flag,
                    mailing_signal=mailing_signal,
                    status='research_needed'
                    if deceased_flag or mailing_signal
                    else 'new',
                    priority=self._priority(
                        deceased_flag, mailing_signal, total_due
                    ),
                    research_status='unreviewed',
                    phone=(
                        str(self._value(row, columns.get('phone'))).strip()
                        or None
                    ),
                    source_job_id=job['uid'],
                    source_data_json=self._record_json(row),
                    created_at=now,
                    updated_at=now,
                )
                session.add(lead)
                session.flush()
                prior_lead = session.scalar(
                    select(Lead).where(
                        Lead.id != lead.id,
                        Lead.source_job_id != job['uid'],
                        or_(
                            and_(
                                Lead.tax_id == tax_id,
                                Lead.tax_id.is_not(None),
                                Lead.tax_id != '',
                            ),
                            and_(
                                Lead.property_address == property_address,
                                Lead.property_city == lead.property_city,
                            ),
                        ),
                    ).order_by(Lead.updated_at.desc())
                )
                if prior_lead:
                    inherited = session.scalars(select(ContactPoint).where(
                        ContactPoint.lead_id == prior_lead.id
                    )).all()
                    for point in inherited:
                        session.add(ContactPoint(
                            lead_id=lead.id, kind=point.kind, value=point.value,
                            normalized_value=point.normalized_value,
                            label=point.label, source_name=point.source_name,
                            confidence=point.confidence, status=point.status,
                            is_primary=point.is_primary, notes=(
                                f'Inherited from prior campaign lead {prior_lead.id}'
                            ), created_at=now, updated_at=now,
                        ))
                    if not lead.phone:
                        lead.phone = prior_lead.phone
                    if not lead.email:
                        lead.email = prior_lead.email
                if lead.phone:
                    existing_phone = session.scalar(select(ContactPoint.id).where(
                        ContactPoint.lead_id == lead.id,
                        ContactPoint.kind == 'phone',
                        ContactPoint.normalized_value == normalize_contact_value('phone', lead.phone),
                    ))
                    if not existing_phone:
                        session.add(
                        ContactPoint(
                            lead_id=lead.id,
                            kind='phone',
                            value=lead.phone,
                            normalized_value=normalize_contact_value(
                                'phone', lead.phone
                            ),
                            label='County list contact',
                            source_name=job['source_filename'],
                            confidence='unverified',
                            status='active',
                            is_primary=True,
                            notes='Imported with the approved source record',
                            created_at=now,
                            updated_at=now,
                        ))
                session.add(
                    LeadActivity(
                        lead_id=lead.id,
                        activity_type='imported',
                        detail=f"Imported from {job['source_filename']}",
                        created_at=now,
                    )
                )
                imported += 1
        return imported

    def list_leads(
        self,
        search='',
        status='',
        priority='',
        research_only=False,
        research_category_filter='',
        follow_up='',
        page=1,
        per_page=50,
        campaign_id=None,
    ):
        conditions = []
        if campaign_id:
            conditions.append(
                Lead.source_job_id == self._campaign_job_id(campaign_id)
            )
        if search:
            term = f'%{search}%'
            conditions.append(
                or_(
                    Lead.owner_name.ilike(term),
                    Lead.property_address.ilike(term),
                    Lead.tax_id.ilike(term),
                )
            )
        if status in CRM_STATUSES:
            conditions.append(Lead.status == status)
        if priority in CRM_PRIORITIES:
            conditions.append(Lead.priority == priority)
        if research_only:
            conditions.append(research_queue_condition())
        if (
            research_category_filter
            and research_category_filter not in RESEARCH_CATEGORIES
        ):
            raise ValueError('Invalid research category')
        if follow_up == 'due':
            conditions.extend(
                (
                    Lead.next_follow_up.is_not(None),
                    Lead.next_follow_up <= date.today().isoformat(),
                    Lead.status.not_in(('closed', 'disqualified')),
                )
            )

        page = max(int(page), 1)
        per_page = min(max(int(per_page), 1), 100)
        latest_note = (
            select(LeadNote.body)
            .where(LeadNote.lead_id == Lead.id)
            .order_by(LeadNote.created_at.desc())
            .limit(1)
            .correlate(Lead)
            .scalar_subquery()
        )
        priority_order = case(
            (Lead.priority == 'urgent', 0),
            (Lead.priority == 'high', 1),
            (Lead.priority == 'medium', 2),
            else_=3,
        )
        ordering = (
            [Lead.next_follow_up.asc(), priority_order]
            if follow_up == 'due'
            else [priority_order]
        )
        ordering.extend(
            (
                Lead.deceased_flag.desc(),
                Lead.total_due.desc(),
                Lead.id.desc(),
            )
        )

        with self.Session() as session:
            research_summary = []
            if research_only:
                queue_leads = session.scalars(
                    select(Lead).where(research_queue_condition())
                ).all()
                category_counts = {
                    category: 0 for category in RESEARCH_CATEGORIES
                }
                for lead in queue_leads:
                    category_counts[research_category(lead)] += 1
                research_summary = [
                    {
                        'category': category,
                        'label': RESEARCH_CATEGORY_LABELS[category],
                        'description': (
                            RESEARCH_CATEGORY_DESCRIPTIONS[category]
                        ),
                        'count': category_counts[category],
                    }
                    for category in RESEARCH_CATEGORIES
                ]

                rows = session.execute(
                    select(Lead, latest_note.label('latest_note'))
                    .where(*conditions)
                ).all()
                if research_category_filter:
                    rows = [
                        row for row in rows
                        if research_category(row[0])
                        == research_category_filter
                    ]
                priority_rank = {
                    'urgent': 0,
                    'high': 1,
                    'medium': 2,
                    'normal': 3,
                }
                category_rank = {
                    category: rank
                    for rank, category in enumerate(RESEARCH_CATEGORIES)
                }
                rows.sort(
                    key=lambda row: (
                        category_rank[research_category(row[0])],
                        priority_rank.get(row[0].priority, 9),
                        -float(row[0].total_due or 0),
                        -row[0].id,
                    )
                )
                total = len(rows)
                offset = (page - 1) * per_page
                rows = rows[offset:offset + per_page]
            else:
                total = session.scalar(
                    select(func.count()).select_from(Lead).where(*conditions)
                ) or 0
                offset = (page - 1) * per_page
                rows = session.execute(
                    select(Lead, latest_note.label('latest_note'))
                    .where(*conditions)
                    .order_by(*ordering)
                    .limit(per_page)
                    .offset(offset)
                ).all()

        return {
            'items': [
                {
                    **_as_dict(lead),
                    'latest_note': note,
                    'research_reason': research_reason(lead),
                    **research_category_payload(lead),
                }
                for lead, note in rows
            ],
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': max((total + per_page - 1) // per_page, 1),
            'research_summary': research_summary,
        }

    def _campaign_job_id(self, campaign_id):
        try:
            campaign_id = int(campaign_id)
        except (TypeError, ValueError) as error:
            raise ValueError('Invalid campaign') from error
        with self.Session() as session:
            job_id = session.scalar(
                select(Campaign.source_job_id).where(Campaign.id == campaign_id)
            )
        if not job_id:
            raise ValueError('Campaign not found')
        return job_id

    def list_campaigns(self, include_archived=True):
        with self.Session() as session:
            conditions = [] if include_archived else [Campaign.status != 'archived']
            campaigns = session.scalars(
                select(Campaign).where(*conditions).order_by(Campaign.created_at.desc())
            ).all()
            counts = dict(session.execute(
                select(Lead.source_job_id, func.count(Lead.id)).group_by(Lead.source_job_id)
            ).all())
        return [
            {**_as_dict(item), 'lead_count': int(counts.get(item.source_job_id, 0))}
            for item in campaigns
        ]

    def update_campaign(self, campaign_id, changes):
        now = utc_now()
        with self.Session.begin() as session:
            campaign = session.get(Campaign, int(campaign_id))
            if not campaign:
                return None
            if 'name' in changes:
                name = str(changes['name'] or '').strip()
                if not name:
                    raise ValueError('Campaign name is required')
                campaign.name = name
            if 'status' in changes:
                if changes['status'] not in CAMPAIGN_STATUSES:
                    raise ValueError('Invalid campaign status')
                campaign.status = changes['status']
            campaign.updated_at = now
        return next(
            item for item in self.list_campaigns() if item['id'] == int(campaign_id)
        )

    def delete_campaign(self, campaign_id, confirmation):
        try:
            campaign_id = int(campaign_id)
        except (TypeError, ValueError) as error:
            raise ValueError('Invalid campaign') from error
        confirmation = str(confirmation or '').strip()
        with self.Session.begin() as session:
            campaign = session.get(Campaign, campaign_id)
            if not campaign:
                return None
            if confirmation != campaign.name:
                raise ValueError('Type the campaign name exactly to delete it')
            active_batch = session.scalar(
                select(EnrichmentBatch.id).where(
                    EnrichmentBatch.campaign_id == campaign_id,
                    EnrichmentBatch.status.in_(('running', 'processing')),
                )
            )
            if active_batch:
                raise ValueError(
                    'This campaign has a skip trace in progress and cannot be deleted'
                )
            lead_ids = select(Lead.id).where(
                Lead.source_job_id == campaign.source_job_id
            )
            lead_count = session.scalar(
                select(func.count()).select_from(Lead).where(
                    Lead.source_job_id == campaign.source_job_id
                )
            ) or 0
            for model in (
                LeadNote,
                LeadActivity,
                CallLog,
                ResearchEvidence,
                ProbateContact,
                ContactPoint,
            ):
                session.execute(
                    delete(model).where(model.lead_id.in_(lead_ids)),
                    execution_options={'synchronize_session': False},
                )
            session.execute(
                delete(Lead).where(
                    Lead.source_job_id == campaign.source_job_id
                ),
                execution_options={'synchronize_session': False},
            )
            session.execute(
                delete(EnrichmentBatch).where(
                    EnrichmentBatch.campaign_id == campaign_id
                ),
                execution_options={'synchronize_session': False},
            )
            session.delete(campaign)
        return {
            'campaign_id': campaign_id,
            'deleted_leads': int(lead_count),
            'processing_history_preserved': True,
        }

    def get_lead(self, lead_id):
        with self.Session() as session:
            lead = session.get(Lead, lead_id)
            if not lead:
                return None
            notes = session.scalars(
                select(LeadNote)
                .where(LeadNote.lead_id == lead_id)
                .order_by(LeadNote.created_at.desc())
            ).all()
            activity = session.scalars(
                select(LeadActivity)
                .where(LeadActivity.lead_id == lead_id)
                .order_by(LeadActivity.created_at.desc())
            ).all()
            calls = session.scalars(
                select(CallLog)
                .where(CallLog.lead_id == lead_id)
                .order_by(CallLog.created_at.desc())
            ).all()
            evidence = session.scalars(
                select(ResearchEvidence)
                .where(ResearchEvidence.lead_id == lead_id)
                .order_by(ResearchEvidence.created_at.desc())
            ).all()
            probate_contacts = session.scalars(
                select(ProbateContact)
                .where(ProbateContact.lead_id == lead_id)
                .order_by(ProbateContact.created_at.desc())
            ).all()
            contact_points = session.scalars(
                select(ContactPoint)
                .where(ContactPoint.lead_id == lead_id)
                .order_by(
                    ContactPoint.is_primary.desc(),
                    ContactPoint.created_at.desc(),
                )
            ).all()
            contact_payload = [_as_dict(item) for item in contact_points]
            recorded = {
                (item['kind'], item['normalized_value'])
                for item in contact_payload
            }
            for kind, value in (('phone', lead.phone), ('email', lead.email)):
                normalized = normalize_contact_value(kind, value)
                if normalized and (kind, normalized) not in recorded:
                    contact_payload.append({
                        'id': None,
                        'lead_id': lead.id,
                        'kind': kind,
                        'value': value,
                        'normalized_value': normalized,
                        'label': 'Legacy primary',
                        'source_name': 'Existing CRM record',
                        'confidence': 'unverified',
                        'status': 'active',
                        'is_primary': True,
                        'notes': (
                            'Original contact field; record a sourced contact '
                            'point to validate it'
                        ),
                        'created_at': None,
                        'updated_at': None,
                    })
            return {
                **_as_dict(lead),
                'research_reason': research_reason(lead),
                **research_category_payload(lead),
                'notes': [_as_dict(note) for note in notes],
                'activity': [_as_dict(item) for item in activity],
                'calls': [_as_dict(item) for item in calls],
                'evidence': [_as_dict(item) for item in evidence],
                'evidence_summary': summarize_evidence(evidence),
                'probate_contacts': [
                    _as_dict(item) for item in probate_contacts
                ],
                'contact_points': contact_payload,
            }

    def list_probate_cases(
        self,
        search='',
        stage='',
        page=1,
        per_page=24,
    ):
        if stage and stage not in PROBATE_STAGE_LABELS:
            raise ValueError('Invalid probate stage')
        page = max(int(page), 1)
        per_page = min(max(int(per_page), 1), 100)
        needle = str(search or '').strip().lower()

        with self.Session() as session:
            leads = session.scalars(
                select(Lead).order_by(Lead.updated_at.desc(), Lead.id.desc())
            ).all()
            evidence = session.scalars(
                select(ResearchEvidence)
                .where(ResearchEvidence.retracted_at.is_(None))
                .order_by(ResearchEvidence.created_at.desc())
            ).all()
            contacts = session.scalars(select(ProbateContact)).all()

        evidence_by_lead = {}
        for item in evidence:
            evidence_by_lead.setdefault(item.lead_id, []).append(item)
        contact_counts = {}
        for item in contacts:
            contact_counts[item.lead_id] = (
                contact_counts.get(item.lead_id, 0) + 1
            )

        all_items = []
        for lead in leads:
            lead_evidence = evidence_by_lead.get(lead.id, [])
            category = research_category(lead)
            if not (
                lead.deceased_flag
                or category == 'care_of_representative'
                or lead_evidence
            ):
                continue
            summary = summarize_evidence(lead_evidence)
            case_stage = probate_case_stage(lead, summary)
            haystack = ' '.join(
                str(value or '').lower()
                for value in (
                    lead.owner_name,
                    lead.property_address,
                    lead.property_city,
                    lead.tax_id,
                )
            )
            lead_data = _as_dict(lead)
            lead_data.pop('source_data_json', None)
            all_items.append({
                **lead_data,
                **research_category_payload(lead),
                'probate_stage': case_stage,
                'probate_stage_label': PROBATE_STAGE_LABELS[case_stage],
                'evidence_summary': summary,
                'evidence_count': len(lead_evidence),
                'probate_contact_count': contact_counts.get(lead.id, 0),
                '_search_text': haystack,
            })

        stage_rank = {
            'needs_resolution': 0,
            'direct_signal': 1,
            'probable': 2,
            'evidence_review': 3,
            'representative_signal': 4,
            'confirmed': 5,
            'false_positive': 6,
        }
        priority_rank = {'urgent': 0, 'high': 1, 'medium': 2, 'normal': 3}
        all_items.sort(
            key=lambda item: (
                stage_rank[item['probate_stage']],
                priority_rank.get(item['priority'], 9),
                -float(item['total_due'] or 0),
                -item['id'],
            )
        )
        counts = {
            stage_name: 0 for stage_name in PROBATE_STAGE_LABELS
        }
        for item in all_items:
            counts[item['probate_stage']] += 1
        items = [
            item for item in all_items
            if (not stage or item['probate_stage'] == stage)
            and (not needle or needle in item['_search_text'])
        ]
        for item in items:
            item.pop('_search_text', None)
        total = len(items)
        offset = (page - 1) * per_page
        return {
            'items': items[offset:offset + per_page],
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': max((total + per_page - 1) // per_page, 1),
            'stages': [
                {
                    'stage': stage_name,
                    'label': label,
                    'count': counts[stage_name],
                }
                for stage_name, label in PROBATE_STAGE_LABELS.items()
            ],
            'methodology': (
                'This workspace contains direct estate signals, '
                'representative-address candidates, and cases with recorded '
                'death or probate evidence. A signal is never presented as '
                'confirmed death without identity-matched official evidence.'
            ),
        }

    def update_lead(self, lead_id, changes):
        allowed = {
            'status': CRM_STATUSES,
            'priority': CRM_PRIORITIES,
            'research_status': RESEARCH_STATUSES,
            'next_follow_up': None,
            'phone': None,
            'email': None,
        }
        updates = {}
        for field, value in changes.items():
            if field not in allowed:
                continue
            choices = allowed[field]
            if choices and value not in choices:
                raise ValueError(f'Invalid {field}')
            updates[field] = value
        if not updates:
            raise ValueError('No valid changes supplied')
        if 'next_follow_up' in updates and updates['next_follow_up']:
            try:
                date.fromisoformat(updates['next_follow_up'])
            except (TypeError, ValueError) as error:
                raise ValueError('Invalid next follow-up date') from error
        if updates.get('status') == 'disqualified':
            updates['next_follow_up'] = None

        now = utc_now()
        with self.Session.begin() as session:
            lead = session.get(Lead, lead_id)
            if not lead:
                return None
            for field, value in updates.items():
                if getattr(lead, field) != value:
                    setattr(lead, field, value)
                    session.add(
                        LeadActivity(
                            lead_id=lead_id,
                            activity_type='updated',
                            detail=(
                                f"{field.replace('_', ' ').title()} changed to "
                                f"{value or 'empty'}"
                            ),
                            created_at=now,
                        )
                    )
            lead.updated_at = now
        return self.get_lead(lead_id)

    def add_note(self, lead_id, body):
        body = (body or '').strip()
        if not body:
            raise ValueError('Note cannot be empty')
        now = utc_now()
        with self.Session.begin() as session:
            if not session.get(Lead, lead_id):
                return None
            note = LeadNote(lead_id=lead_id, body=body, created_at=now)
            session.add(note)
            session.flush()
            note_id = note.id
            session.add(
                LeadActivity(
                    lead_id=lead_id,
                    activity_type='note_added',
                    detail='Note added',
                    created_at=now,
                )
            )
        return {'id': note_id, 'lead_id': lead_id, 'body': body, 'created_at': now}

    def log_call(self, lead_id, payload):
        direction = str(payload.get('direction', 'outbound')).strip()
        outcome = str(payload.get('outcome', '')).strip()
        phone_number = str(payload.get('phone_number', '')).strip() or None
        notes = str(payload.get('notes', '')).strip() or None
        next_follow_up = str(payload.get('next_follow_up', '')).strip() or None
        if direction not in CALL_DIRECTIONS:
            raise ValueError('Invalid call direction')
        if outcome not in CALL_OUTCOMES:
            raise ValueError('Invalid call outcome')
        if outcome in CALL_OUTCOMES_REQUIRING_FOLLOW_UP and not next_follow_up:
            raise ValueError('This outcome requires a next follow-up date')
        if next_follow_up:
            try:
                follow_up_date = date.fromisoformat(next_follow_up)
            except ValueError as error:
                raise ValueError('Invalid next follow-up date') from error
            if follow_up_date < date.today():
                raise ValueError('Next follow-up cannot be in the past')
        duration = payload.get('duration_minutes')
        if duration in (None, ''):
            duration = None
        else:
            try:
                duration = int(duration)
            except (TypeError, ValueError) as error:
                raise ValueError('Call duration must be a whole number') from error
            if duration < 0 or duration > 1440:
                raise ValueError('Call duration must be between 0 and 1440 minutes')

        now = utc_now()
        with self.Session.begin() as session:
            lead = session.get(Lead, lead_id)
            if not lead:
                return None
            if phone_number:
                normalized_phone = normalize_contact_value(
                    'phone', phone_number
                )
                blocked = session.scalar(
                    select(ContactPoint.id).where(
                        ContactPoint.lead_id == lead_id,
                        ContactPoint.kind == 'phone',
                        ContactPoint.normalized_value == normalized_phone,
                        ContactPoint.status == 'do_not_contact',
                    )
                )
                if blocked:
                    raise ValueError('This phone is marked Do Not Contact')
            call = CallLog(
                lead_id=lead_id,
                direction=direction,
                outcome=outcome,
                phone_number=phone_number,
                duration_minutes=duration,
                notes=notes,
                next_follow_up=next_follow_up,
                created_at=now,
            )
            session.add(call)
            lead.status = CALL_OUTCOME_STATUSES[outcome]
            lead.last_contacted_at = now.isoformat()
            lead.next_follow_up = (
                None if outcome == 'not_interested'
                else next_follow_up
            )
            lead.updated_at = now
            session.add(
                LeadActivity(
                    lead_id=lead_id,
                    activity_type='call_logged',
                    detail=(
                        f"{direction.title()} call · "
                        f"{outcome.replace('_', ' ').title()}"
                    ),
                    created_at=now,
                )
            )
            session.flush()
            call_id = call.id
        return {'call_id': call_id, 'lead': self.get_lead(lead_id)}

    def add_evidence(self, lead_id, payload):
        evidence_type = str(payload.get('evidence_type', '')).strip()
        outcome = str(payload.get('outcome', '')).strip()
        confidence = str(payload.get('confidence', '')).strip()
        identity_match = str(payload.get('identity_match', '')).strip()
        source_name = str(payload.get('source_name', '')).strip()
        if evidence_type not in EVIDENCE_TYPES:
            raise ValueError('Invalid evidence type')
        if outcome not in EVIDENCE_OUTCOMES:
            raise ValueError('Invalid evidence outcome')
        if confidence not in EVIDENCE_CONFIDENCE:
            raise ValueError('Invalid evidence confidence')
        if identity_match not in IDENTITY_MATCHES:
            raise ValueError('Invalid identity match')
        if not source_name:
            raise ValueError('Evidence source is required')

        source_url = str(payload.get('source_url', '')).strip() or None
        if source_url and not source_url.startswith(('https://', 'http://')):
            raise ValueError('Evidence URL must start with http:// or https://')
        now = utc_now()
        with self.Session.begin() as session:
            lead = session.get(Lead, lead_id)
            if not lead:
                return None
            evidence = ResearchEvidence(
                lead_id=lead_id,
                evidence_type=evidence_type,
                outcome=outcome,
                confidence=confidence,
                identity_match=identity_match,
                source_name=source_name,
                source_url=source_url,
                case_number=str(payload.get('case_number', '')).strip() or None,
                subject_name=str(payload.get('subject_name', '')).strip() or None,
                event_date=str(payload.get('event_date', '')).strip() or None,
                notes=str(payload.get('notes', '')).strip() or None,
                created_at=now,
            )
            session.add(evidence)
            session.flush()
            evidence_id = evidence.id
            session.add(
                LeadActivity(
                    lead_id=lead_id,
                    activity_type='evidence_added',
                    detail=(
                        f"Evidence added: {evidence_type.replace('_', ' ')} "
                        f"({outcome.replace('_', ' ')})"
                    ),
                    created_at=now,
                )
            )
            all_evidence = session.scalars(
                select(ResearchEvidence).where(
                    ResearchEvidence.lead_id == lead_id
                )
            ).all()
            summary = summarize_evidence(all_evidence)
            if summary['confirmed']:
                lead.research_status = 'verified'
            elif summary['status'] == 'false_positive':
                lead.research_status = 'rejected'
            else:
                lead.research_status = 'in_progress'
            lead.updated_at = now
        detail = self.get_lead(lead_id)
        return {
            'evidence_id': evidence_id,
            'lead': detail,
        }

    def add_probate_contact(self, lead_id, payload):
        name = str(payload.get('name', '')).strip()
        role = str(payload.get('role', '')).strip()
        source_name = str(payload.get('source_name', '')).strip()
        if not name:
            raise ValueError('Probate contact name is required')
        if role not in PROBATE_CONTACT_ROLES:
            raise ValueError('Invalid probate contact role')
        if not source_name:
            raise ValueError('Probate contact source is required')
        source_url = str(payload.get('source_url', '')).strip() or None
        if source_url and not source_url.startswith(('https://', 'http://')):
            raise ValueError('Source URL must start with http:// or https://')
        now = utc_now()
        with self.Session.begin() as session:
            lead = session.get(Lead, lead_id)
            if not lead:
                return None
            contact = ProbateContact(
                lead_id=lead_id,
                name=name,
                role=role,
                phone=str(payload.get('phone', '')).strip() or None,
                email=str(payload.get('email', '')).strip() or None,
                source_name=source_name,
                source_url=source_url,
                notes=str(payload.get('notes', '')).strip() or None,
                created_at=now,
            )
            session.add(contact)
            session.flush()
            contact_id = contact.id
            session.add(
                LeadActivity(
                    lead_id=lead_id,
                    activity_type='probate_contact_added',
                    detail=(
                        f"Probate contact added: {name} "
                        f"({role.replace('_', ' ')})"
                    ),
                    created_at=now,
                )
            )
            lead.updated_at = now
        return {
            'contact_id': contact_id,
            'lead': self.get_lead(lead_id),
        }

    def add_contact_point(self, lead_id, payload):
        kind = str(payload.get('kind') or '').strip().lower()
        value = str(payload.get('value') or '').strip()
        source_name = str(payload.get('source_name') or '').strip()
        confidence = str(payload.get('confidence') or 'unverified').strip().lower()
        status = str(payload.get('status') or 'active').strip().lower()
        if kind not in CONTACT_KINDS:
            raise ValueError('Contact kind must be phone or email')
        normalized = normalize_contact_value(kind, value)
        if not value or not normalized:
            raise ValueError('Contact value is required')
        if kind == 'phone' and len(normalized) < 10:
            raise ValueError('A valid phone number is required')
        if kind == 'email' and not re.fullmatch(
            r'[^@\s]+@[^@\s]+\.[^@\s]+', normalized
        ):
            raise ValueError('A valid email address is required')
        if not source_name:
            raise ValueError('Contact source is required')
        if confidence not in CONTACT_CONFIDENCE:
            raise ValueError('Invalid contact confidence')
        if status not in CONTACT_STATUSES:
            raise ValueError('Invalid contact status')
        now = utc_now()
        with self.Session.begin() as session:
            lead = session.get(Lead, lead_id)
            if not lead:
                return None
            contact = session.scalar(select(ContactPoint).where(
                ContactPoint.lead_id == lead_id,
                ContactPoint.kind == kind,
                ContactPoint.normalized_value == normalized,
            ))
            legacy_value = lead.phone if kind == 'phone' else lead.email
            make_primary = (
                bool(payload.get('is_primary'))
                or not legacy_value
                or normalize_contact_value(kind, legacy_value) == normalized
            )
            if contact:
                contact.source_name = source_name
                contact.confidence = confidence
                contact.status = status
                contact.label = str(payload.get('label') or '').strip() or None
                contact.notes = str(payload.get('notes') or '').strip() or None
                contact.updated_at = now
            else:
                contact = ContactPoint(
                    lead_id=lead_id,
                    kind=kind,
                    value=value,
                    normalized_value=normalized,
                    label=str(payload.get('label') or '').strip() or None,
                    source_name=source_name,
                    confidence=confidence,
                    status=status,
                    is_primary=False,
                    notes=str(payload.get('notes') or '').strip() or None,
                    created_at=now,
                    updated_at=now,
                )
                session.add(contact)
                session.flush()
            if make_primary and status == 'active':
                session.execute(update(ContactPoint).where(
                    ContactPoint.lead_id == lead_id,
                    ContactPoint.kind == kind,
                    ContactPoint.id != contact.id,
                ).values(is_primary=False))
                contact.is_primary = True
                if kind == 'phone':
                    lead.phone = value
                else:
                    lead.email = value
            lead.updated_at = now
            contact_id = contact.id
            session.add(LeadActivity(
                lead_id=lead_id,
                activity_type='contact_point_recorded',
                detail=f"{kind.title()} recorded from {source_name}",
                created_at=now,
            ))
        return {'contact_id': contact_id, 'lead': self.get_lead(lead_id)}

    def update_contact_point(self, lead_id, contact_id, payload):
        now = utc_now()
        with self.Session.begin() as session:
            lead = session.get(Lead, lead_id)
            contact = session.get(ContactPoint, contact_id)
            if not lead or not contact or contact.lead_id != lead_id:
                return None
            status = str(payload.get('status') or contact.status).strip().lower()
            confidence = str(payload.get('confidence') or contact.confidence).strip().lower()
            if status not in CONTACT_STATUSES:
                raise ValueError('Invalid contact status')
            if confidence not in CONTACT_CONFIDENCE:
                raise ValueError('Invalid contact confidence')
            contact.status = status
            contact.confidence = confidence
            contact.updated_at = now
            if payload.get('is_primary') and status == 'active':
                session.execute(update(ContactPoint).where(
                    ContactPoint.lead_id == lead_id,
                    ContactPoint.kind == contact.kind,
                    ContactPoint.id != contact.id,
                ).values(is_primary=False))
                contact.is_primary = True
                if contact.kind == 'phone':
                    lead.phone = contact.value
                else:
                    lead.email = contact.value
            elif status != 'active' and contact.is_primary:
                contact.is_primary = False
                replacement = session.scalar(
                    select(ContactPoint).where(
                        ContactPoint.lead_id == lead_id,
                        ContactPoint.kind == contact.kind,
                        ContactPoint.status == 'active',
                        ContactPoint.id != contact.id,
                    ).order_by(
                        case(
                            (ContactPoint.confidence == 'verified', 0),
                            (ContactPoint.confidence == 'probable', 1),
                            else_=2,
                        ),
                        ContactPoint.created_at.desc(),
                    )
                )
                if replacement:
                    replacement.is_primary = True
                replacement_value = replacement.value if replacement else None
                if contact.kind == 'phone':
                    lead.phone = replacement_value
                else:
                    lead.email = replacement_value
            if status == 'do_not_contact' and contact.kind == 'phone':
                lead.next_follow_up = None
            lead.updated_at = now
            session.add(LeadActivity(
                lead_id=lead_id,
                activity_type='contact_point_updated',
                detail=f"{contact.kind.title()} marked {status}",
                created_at=now,
            ))
        return {'lead': self.get_lead(lead_id)}

    def retract_evidence(self, lead_id, evidence_id, reason):
        reason = str(reason or '').strip()
        if not reason:
            raise ValueError('Retraction reason is required')
        now = utc_now()
        with self.Session.begin() as session:
            lead = session.get(Lead, lead_id)
            if not lead:
                return None
            evidence = session.get(ResearchEvidence, evidence_id)
            if not evidence or evidence.lead_id != lead_id:
                return None
            if not evidence.retracted_at:
                evidence.retracted_at = now
                evidence.retraction_reason = reason
                session.add(
                    LeadActivity(
                        lead_id=lead_id,
                        activity_type='evidence_retracted',
                        detail=(
                            f"Evidence retracted: "
                            f"{evidence.evidence_type.replace('_', ' ')}"
                        ),
                        created_at=now,
                    )
                )
            active = session.scalars(
                select(ResearchEvidence).where(
                    ResearchEvidence.lead_id == lead_id
                )
            ).all()
            summary = summarize_evidence(active)
            if summary['confirmed']:
                lead.research_status = 'verified'
            elif summary['status'] == 'false_positive':
                lead.research_status = 'rejected'
            elif summary['status'] == 'no_evidence':
                lead.research_status = 'unreviewed'
            else:
                lead.research_status = 'in_progress'
            lead.updated_at = now
        return self.get_lead(lead_id)

    def dashboard_metrics(self, campaign_id=None):
        today = date.today().isoformat()
        campaign_conditions = []
        if campaign_id:
            campaign_conditions.append(
                Lead.source_job_id == self._campaign_job_id(campaign_id)
            )
        with self.Session() as session:
            total = session.scalar(
                select(func.count()).select_from(Lead).where(*campaign_conditions)
            ) or 0
            deceased = session.scalar(
                select(func.count())
                .select_from(Lead)
                .where(*campaign_conditions, Lead.deceased_flag.is_(True))
            ) or 0
            research = session.scalar(
                select(func.count())
                .select_from(Lead)
                .where(*campaign_conditions, research_queue_condition())
            ) or 0
            contacts = session.scalar(
                select(func.count())
                .select_from(Lead)
                .where(*campaign_conditions,
                    or_(
                        func.coalesce(func.trim(Lead.phone), '') != '',
                        func.coalesce(func.trim(Lead.email), '') != '',
                    )
                )
            ) or 0
            overdue = session.scalar(
                select(func.count())
                .select_from(Lead)
                .where(*campaign_conditions,
                    Lead.next_follow_up.is_not(None),
                    Lead.next_follow_up < today,
                    Lead.status.not_in(('closed', 'disqualified')),
                )
            ) or 0
            due_today = session.scalar(
                select(func.count())
                .select_from(Lead)
                .where(*campaign_conditions,
                    Lead.next_follow_up.is_not(None),
                    Lead.next_follow_up <= today,
                    Lead.status.not_in(('closed', 'disqualified')),
                )
            ) or 0
        return {
            'actionable_leads': total,
            'deceased_signals': deceased,
            'research_queue': research,
            'contacts_found': contacts,
            'overdue_follow_ups': overdue,
            'follow_ups_due': due_today,
        }

    def pipeline_board(self, cards_per_stage=50, campaign_id=None):
        cards_per_stage = min(max(int(cards_per_stage), 1), 100)
        priority_order = case(
            (Lead.priority == 'urgent', 0),
            (Lead.priority == 'high', 1),
            (Lead.priority == 'medium', 2),
            else_=3,
        )
        stages = []
        campaign_conditions = []
        if campaign_id:
            campaign_conditions.append(
                Lead.source_job_id == self._campaign_job_id(campaign_id)
            )
        with self.Session() as session:
            for status in PIPELINE_STAGES:
                count, debt = session.execute(
                    select(
                        func.count(Lead.id),
                        func.coalesce(func.sum(Lead.total_due), 0),
                    ).where(*campaign_conditions, Lead.status == status)
                ).one()
                leads = session.scalars(
                    select(Lead)
                    .where(*campaign_conditions, Lead.status == status)
                    .order_by(
                        priority_order,
                        Lead.next_follow_up.asc(),
                        Lead.total_due.desc(),
                        Lead.id.desc(),
                    )
                    .limit(cards_per_stage)
                ).all()
                stages.append(
                    {
                        'status': status,
                        'count': int(count),
                        'total_debt': float(debt),
                        'items': [_as_dict(lead) for lead in leads],
                    }
                )
            disqualified = session.scalar(
                select(func.count())
                .select_from(Lead)
                .where(*campaign_conditions, Lead.status == 'disqualified')
            ) or 0
        return {
            'stages': stages,
            'disqualified': int(disqualified),
            'cards_per_stage': cards_per_stage,
            'generated_at': utc_now().isoformat(),
        }

    def list_properties(self, search='', status='', page=1, per_page=24):
        """Return one current workspace card per property identity.

        County files can contain the same parcel in more than one import. The
        newest lead is the operational record; older rows remain available as
        source history and are never added together as if they were new debt.
        """
        if status and status not in CRM_STATUSES:
            raise ValueError('Invalid status')
        page = max(int(page), 1)
        per_page = min(max(int(per_page), 1), 100)
        needle = str(search or '').strip().lower()

        with self.Session() as session:
            leads = session.scalars(
                select(Lead).order_by(Lead.updated_at.desc(), Lead.id.desc())
            ).all()
            evidence_counts = dict(
                session.execute(
                    select(
                        ResearchEvidence.lead_id,
                        func.count(ResearchEvidence.id),
                    )
                    .where(ResearchEvidence.retracted_at.is_(None))
                    .group_by(ResearchEvidence.lead_id)
                ).all()
            )
            call_counts = dict(
                session.execute(
                    select(CallLog.lead_id, func.count(CallLog.id))
                    .group_by(CallLog.lead_id)
                ).all()
            )
            assessor = {
                row.account_no.strip().upper(): _as_dict(row)
                for row in session.scalars(select(AssessorVerification)).all()
            }

        grouped = {}
        for lead in leads:
            identity = _lead_property_identity(lead)
            if identity not in grouped:
                lead_data = _as_dict(lead)
                lead_data.pop('source_data_json', None)
                context = _lead_property_context(lead)
                assessor_record = assessor.get(
                    _lead_assessor_account_no(lead)
                )
                grouped[identity] = {
                    **lead_data,
                    **context,
                    'lead_id': lead.id,
                    'record_count': 0,
                    'tax_years': [],
                    'evidence_count': 0,
                    'call_count': 0,
                    'assessor_verification': assessor_record,
                }
            item = grouped[identity]
            item['record_count'] += 1
            item['evidence_count'] += evidence_counts.get(lead.id, 0)
            item['call_count'] += call_counts.get(lead.id, 0)
            if lead.tax_year not in item['tax_years']:
                item['tax_years'].append(lead.tax_year)

        items = []
        for item in grouped.values():
            item['tax_years'].sort(reverse=True)
            haystack = ' '.join(
                str(item.get(field) or '').lower()
                for field in (
                    'owner_name',
                    'property_address',
                    'property_city',
                    'tax_id',
                    'parcel_id',
                    'tax_ids',
                )
            )
            if needle and needle not in haystack:
                continue
            if status and item['status'] != status:
                continue
            items.append(item)

        priority_rank = {'urgent': 0, 'high': 1, 'medium': 2, 'normal': 3}
        items.sort(
            key=lambda item: (
                priority_rank.get(item['priority'], 9),
                -float(item['total_due'] or 0),
                -item['lead_id'],
            )
        )
        total = len(items)
        offset = (page - 1) * per_page
        return {
            'items': items[offset:offset + per_page],
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': max((total + per_page - 1) // per_page, 1),
            'methodology': (
                'One card per Tulsa County parcel/PID. Multiple tax accounts '
                'remain attached to the same property and their verified debt '
                'is shown as one consolidated total.'
            ),
        }

    def acquisition_report(self):
        """Build a transparent operational report from persisted CRM facts."""
        today = date.today().isoformat()
        with self.Session() as session:
            all_leads = session.scalars(
                select(Lead).order_by(Lead.updated_at.desc(), Lead.id.desc())
            ).all()
            evidence = session.scalars(
                select(ResearchEvidence)
                .order_by(ResearchEvidence.created_at.desc())
            ).all()
            call_rows = session.execute(
                select(CallLog.outcome, func.count(CallLog.id))
                .group_by(CallLog.outcome)
            ).all()
            import_count = session.scalar(
                select(func.count()).select_from(ImportRun)
            ) or 0

        current_by_property = {}
        for lead in all_leads:
            identity = _lead_property_identity(lead)
            current_by_property.setdefault(identity, lead)
        leads = list(current_by_property.values())

        evidence_by_lead = {}
        for item in evidence:
            evidence_by_lead.setdefault(item.lead_id, []).append(item)
        evidence_summaries = {
            lead.id: summarize_evidence(evidence_by_lead.get(lead.id, []))
            for lead in leads
        }

        active = [
            lead for lead in leads
            if lead.status not in ('closed', 'disqualified')
        ]
        contactable = [
            lead for lead in active if lead.phone or lead.email
        ]
        follow_ups_due = [
            lead for lead in active
            if lead.next_follow_up and lead.next_follow_up <= today
        ]
        overdue = [
            lead for lead in active
            if lead.next_follow_up and lead.next_follow_up < today
        ]
        confirmed_deceased = sum(
            1 for summary in evidence_summaries.values()
            if summary['confirmed']
        )
        conflicting_evidence = sum(
            1 for summary in evidence_summaries.values()
            if summary['status'] == 'conflicting_evidence'
        )
        total_debt = sum(float(lead.total_due or 0) for lead in active)
        contact_rate = (
            round(len(contactable) / len(active) * 100, 1)
            if active else 0
        )

        stages = {
            status: {'status': status, 'count': 0, 'total_debt': 0.0}
            for status in CRM_STATUSES
        }
        for lead in leads:
            stages[lead.status]['count'] += 1
            stages[lead.status]['total_debt'] += float(lead.total_due or 0)

        research = {status: 0 for status in RESEARCH_STATUSES}
        for lead in leads:
            research[lead.research_status] += 1
        calls = {outcome: 0 for outcome in CALL_OUTCOMES}
        calls.update({outcome: int(count) for outcome, count in call_rows})
        priority_rank = {'urgent': 0, 'high': 1, 'medium': 2, 'normal': 3}
        top_leads = sorted(
            active,
            key=lambda lead: (
                priority_rank.get(lead.priority, 9),
                not lead.deceased_flag,
                -float(lead.total_due or 0),
                -lead.id,
            ),
        )[:8]

        actions = []
        if follow_ups_due:
            actions.append({
                'priority': 'urgent' if overdue else 'high',
                'title': 'Clear the follow-up queue',
                'detail': (
                    f"{len(follow_ups_due)} active follow-ups are due; "
                    f"{len(overdue)} are overdue."
                ),
                'href': '/today',
                'cta': 'Open Today',
            })
        missing_contacts = max(len(active) - len(contactable), 0)
        if missing_contacts:
            actions.append({
                'priority': 'high',
                'title': 'Close the contact-data gap',
                'detail': (
                    f"{missing_contacts} active leads have no phone or email "
                    "saved in the CRM."
                ),
                'href': '/leads',
                'cta': 'Review leads',
            })
        research_open = research['unreviewed'] + research['in_progress']
        if research_open:
            actions.append({
                'priority': 'medium',
                'title': 'Resolve evidence before outreach',
                'detail': (
                    f"{research_open} leads still need evidence review; "
                    f"{conflicting_evidence} contain conflicting evidence."
                ),
                'href': '/research',
                'cta': 'Open research',
            })
        if not actions:
            actions.append({
                'priority': 'normal',
                'title': 'Pipeline is operationally clear',
                'detail': 'No overdue follow-ups or open data gaps were found.',
                'href': '/pipeline',
                'cta': 'Review pipeline',
            })

        return {
            'generated_at': utc_now().isoformat(),
            'methodology': (
                'This report uses the latest CRM record per property and '
                'recorded activity. Repeated imports are not added together. '
                'It does not estimate revenue, property value, or probability '
                'of closing.'
            ),
            'summary': {
                'active_leads': len(active),
                'active_debt': total_debt,
                'contactable_leads': len(contactable),
                'contact_rate': contact_rate,
                'follow_ups_due': len(follow_ups_due),
                'overdue_follow_ups': len(overdue),
                'confirmed_deceased': confirmed_deceased,
                'conflicting_evidence': conflicting_evidence,
                'imports': int(import_count),
            },
            'stages': [stages[status] for status in CRM_STATUSES],
            'research': research,
            'calls': calls,
            'actions': actions,
            'top_opportunities': [
                {
                    **{
                        key: value for key, value in _as_dict(lead).items()
                        if key != 'source_data_json'
                    },
                    'evidence_summary': evidence_summaries[lead.id],
                }
                for lead in top_leads
            ],
        }

    def enrichment_summary(self, campaign_id=None):
        campaign_conditions = []
        if campaign_id:
            campaign_conditions.append(
                Lead.source_job_id == self._campaign_job_id(campaign_id)
            )
        with self.Session() as session:
            missing = session.scalar(
                select(func.count()).select_from(Lead).where(
                    *campaign_conditions,
                    Lead.status.not_in(('closed', 'disqualified')),
                    or_(Lead.phone.is_(None), func.trim(Lead.phone) == ''),
                    or_(Lead.email.is_(None), func.trim(Lead.email) == ''),
                )
            ) or 0
            batches = session.scalars(
                select(EnrichmentBatch)
                .order_by(EnrichmentBatch.created_at.desc())
                .limit(20)
            ).all()
        return {
            'eligible_leads': int(missing),
            'batches': [
                {
                    **_as_dict(batch),
                    'lead_count': len(json.loads(batch.lead_ids_json)),
                    'result_summary': (
                        json.loads(batch.result_summary_json)
                        if batch.result_summary_json else None
                    ),
                }
                for batch in batches
            ],
        }

    def create_enrichment_batch(
        self,
        provider,
        cost_per_record,
        budget_cap,
        max_records=5000,
        campaign_id=None,
        selection_mode='high_priority',
        lead_ids=None,
    ):
        provider = str(provider or 'Tracerfy').strip()
        try:
            cost = round(float(cost_per_record), 4)
            budget = round(float(budget_cap), 2)
            limit = min(max(int(max_records), 1), 10000)
        except (TypeError, ValueError) as error:
            raise ValueError('Invalid cost, budget, or record limit') from error
        if cost <= 0 or cost > 10:
            raise ValueError('Cost per record must be between $0.0001 and $10')
        if budget <= 0:
            raise ValueError('Budget cap must be greater than zero')
        affordable = min(limit, int(budget // cost))
        if affordable < 1:
            raise ValueError('Budget cap does not cover one record')
        if selection_mode not in SKIP_TRACE_SELECTIONS:
            raise ValueError('Invalid skip trace selection')
        if not campaign_id:
            raise ValueError('Select a campaign before skip tracing')
        source_job_id = self._campaign_job_id(campaign_id)
        try:
            requested_ids = {int(item) for item in (lead_ids or [])}
        except (TypeError, ValueError) as error:
            raise ValueError('Invalid manual lead selection') from error
        if selection_mode == 'manual' and not requested_ids:
            raise ValueError('Select at least one lead')

        priority_order = case(
            (Lead.priority == 'urgent', 0),
            (Lead.priority == 'high', 1),
            (Lead.priority == 'medium', 2),
            else_=3,
        )
        now = utc_now()
        with self.Session.begin() as session:
            selection_conditions = [Lead.source_job_id == source_job_id]
            if selection_mode == 'high_priority':
                selection_conditions.append(Lead.priority.in_(('urgent', 'high')))
            elif selection_mode == 'potentially_deceased':
                selection_conditions.append(or_(
                    Lead.deceased_flag.is_(True), Lead.status == 'research_needed'
                ))
            else:
                selection_conditions.append(Lead.id.in_(requested_ids))
            leads = session.scalars(
                select(Lead).where(
                    *selection_conditions,
                    Lead.status.not_in(('closed', 'disqualified')),
                    or_(Lead.phone.is_(None), func.trim(Lead.phone) == ''),
                    or_(Lead.email.is_(None), func.trim(Lead.email) == ''),
                ).order_by(
                    priority_order,
                    Lead.deceased_flag.desc(),
                    Lead.total_due.desc(),
                    Lead.id.desc(),
                ).limit(affordable)
            ).all()
            if not leads:
                raise ValueError('No eligible leads need contact enrichment')
            batch_id = f"enr-{uuid.uuid4().hex[:10]}"
            batch = EnrichmentBatch(
                batch_id=batch_id,
                provider=provider,
                status='pending',
                lead_ids_json=json.dumps([lead.id for lead in leads]),
                cost_per_record=cost,
                budget_cap=budget,
                estimated_cost=round(len(leads) * cost, 2),
                created_at=now,
                updated_at=now,
                campaign_id=int(campaign_id),
                selection_mode=selection_mode,
            )
            session.add(batch)
        return self.get_enrichment_batch(batch_id)

    def get_enrichment_batch(self, batch_id, include_leads=False):
        with self.Session() as session:
            batch = session.scalar(
                select(EnrichmentBatch).where(
                    EnrichmentBatch.batch_id == batch_id
                )
            )
            if not batch:
                return None
            lead_ids = json.loads(batch.lead_ids_json)
            result = {
                **_as_dict(batch),
                'lead_count': len(lead_ids),
                'result_summary': (
                    json.loads(batch.result_summary_json)
                    if batch.result_summary_json else None
                ),
            }
            if include_leads:
                leads = session.scalars(
                    select(Lead).where(Lead.id.in_(lead_ids))
                ).all()
                by_id = {lead.id: lead for lead in leads}
                result['leads'] = [
                    {
                        'Lead ID': by_id[lead_id].id,
                        'Tax ID': by_id[lead_id].tax_id,
                        'Owner Name': by_id[lead_id].owner_name,
                        'Property Address': by_id[lead_id].property_address,
                        'Property City': by_id[lead_id].property_city,
                        'Mailing Address': by_id[lead_id].mailing_address,
                        'ZIP': by_id[lead_id].zip_code,
                        'Phone': '',
                        'Email': '',
                    }
                    for lead_id in lead_ids if lead_id in by_id
                ]
            return result

    def execute_skip_trace_batch(self, batch_id, provider):
        batch = self.get_enrichment_batch(batch_id, include_leads=True)
        if not batch:
            return None
        if batch['status'] not in ('pending', 'failed'):
            raise ValueError('Skip trace batch has already been executed')
        results, failures, actual_cost = [], [], 0.0

        def lookup(lead):
            return lead, provider.lookup({
                    'property_address': lead['Property Address'],
                    'property_city': lead['Property City'],
                    'property_state': 'OK',
                    'zip_code': lead.get('ZIP'),
                })

        # Small selective batches stay within normal web request time without
        # turning this into an unsafe full-list background job.
        with ThreadPoolExecutor(max_workers=min(8, len(batch['leads']))) as pool:
            futures = {pool.submit(lookup, lead): lead for lead in batch['leads']}
            for future in as_completed(futures):
                lead = futures[future]
                try:
                    _, result = future.result()
                except Exception as error:
                    failures.append({'lead_id': lead['Lead ID'], 'error': str(error)})
                    continue
                if result.hit:
                    actual_cost += provider.cost_per_hit
                results.append({
                    'Lead ID': lead['Lead ID'], 'phones': list(result.phones),
                    'emails': list(result.emails), 'deceased': result.deceased,
                })
        self.apply_skip_trace_results(batch_id, results)
        with self.Session.begin() as session:
            item = session.scalar(select(EnrichmentBatch).where(
                EnrichmentBatch.batch_id == batch_id
            ))
            item.actual_cost = round(actual_cost, 2)
            item.error_json = json.dumps(failures) if failures else None
            if failures:
                item.status = 'completed_with_errors' if results else 'failed'
            item.updated_at = utc_now()
        return self.get_enrichment_batch(batch_id)

    def apply_skip_trace_results(self, batch_id, rows):
        now = utc_now()
        with self.Session.begin() as session:
            batch = session.scalar(select(EnrichmentBatch).where(
                EnrichmentBatch.batch_id == batch_id
            ))
            if not batch:
                return None
            allowed_ids = set(json.loads(batch.lead_ids_json))
            summary = {
                'rows_received': len(rows), 'leads_updated': 0,
                'conflicts': 0, 'no_data': 0, 'invalid_rows': 0,
                'dnc_phones': 0,
            }
            for row in rows:
                try:
                    lead_id = int(row.get('Lead ID'))
                except (TypeError, ValueError):
                    summary['invalid_rows'] += 1
                    continue
                if lead_id not in allowed_ids:
                    summary['invalid_rows'] += 1
                    continue
                lead = session.get(Lead, lead_id)
                points = []
                for raw in row.get('phones') or []:
                    value = str(raw.get('number') or raw.get('phone') or '').strip()
                    if value:
                        points.append(('phone', value, raw))
                for raw in row.get('emails') or []:
                    data = raw if isinstance(raw, dict) else {}
                    value = str(data.get('email') or data.get('address') or raw).strip()
                    if value:
                        points.append(('email', value, data))
                if not points:
                    summary['no_data'] += 1
                    continue
                for kind, value, raw in points:
                    normalized = normalize_contact_value(kind, value)
                    if not normalized:
                        continue
                    blocked = kind == 'phone' and bool(
                        raw.get('dnc') or raw.get('tcpa') or raw.get('litigator')
                    )
                    status = 'do_not_contact' if blocked else 'active'
                    if blocked:
                        summary['dnc_phones'] += 1
                    existing = session.scalar(select(ContactPoint).where(
                        ContactPoint.lead_id == lead.id,
                        ContactPoint.kind == kind,
                        ContactPoint.normalized_value == normalized,
                    ))
                    if existing:
                        if blocked:
                            existing.status = 'do_not_contact'
                        existing.updated_at = now
                        continue
                    legacy = lead.phone if kind == 'phone' else lead.email
                    primary = not legacy and not blocked
                    session.add(ContactPoint(
                        lead_id=lead.id, kind=kind, value=value,
                        normalized_value=normalized,
                        label=str(raw.get('type') or 'Skip trace result'),
                        source_name=batch.provider, confidence='probable',
                        status=status, is_primary=primary,
                        notes='Retrieved through selective API skip trace',
                        created_at=now, updated_at=now,
                    ))
                    if primary:
                        if kind == 'phone':
                            lead.phone = value
                        else:
                            lead.email = value
                if row.get('deceased') is True:
                    lead.deceased_flag = True
                    if lead.status == 'new':
                        lead.status = 'research_needed'
                lead.updated_at = now
                summary['leads_updated'] += 1
                session.add(LeadActivity(
                    lead_id=lead.id, activity_type='skip_trace_completed',
                    detail=f'Selective skip trace completed by {batch.provider}',
                    created_at=now,
                ))
            batch.status = 'completed'
            batch.result_summary_json = json.dumps(summary)
            batch.updated_at = now
        return self.get_enrichment_batch(batch_id)

    def apply_enrichment_results(self, batch_id, rows):
        now = utc_now()
        with self.Session.begin() as session:
            batch = session.scalar(
                select(EnrichmentBatch).where(
                    EnrichmentBatch.batch_id == batch_id
                )
            )
            if not batch:
                return None
            allowed_ids = set(json.loads(batch.lead_ids_json))
            summary = {
                'rows_received': 0,
                'leads_updated': 0,
                'conflicts': 0,
                'no_data': 0,
                'invalid_rows': 0,
            }
            for row in rows:
                summary['rows_received'] += 1
                try:
                    lead_id = int(row.get('Lead ID') or row.get('lead_id'))
                except (TypeError, ValueError):
                    summary['invalid_rows'] += 1
                    continue
                if lead_id not in allowed_ids:
                    summary['invalid_rows'] += 1
                    continue
                lead = session.get(Lead, lead_id)
                phone = str(row.get('Phone') or row.get('phone') or '').strip()
                email = str(row.get('Email') or row.get('email') or '').strip()
                if not phone and not email:
                    summary['no_data'] += 1
                    continue
                conflict = (
                    (
                        phone and lead.phone
                        and normalize_contact_value('phone', phone)
                        != normalize_contact_value('phone', lead.phone)
                    )
                    or (
                        email and lead.email
                        and normalize_contact_value('email', email)
                        != normalize_contact_value('email', lead.email)
                    )
                )
                added_point = False
                for kind, value in (('phone', phone), ('email', email)):
                    if not value:
                        continue
                    normalized = normalize_contact_value(kind, value)
                    existing_point = session.scalar(select(ContactPoint).where(
                        ContactPoint.lead_id == lead.id,
                        ContactPoint.kind == kind,
                        ContactPoint.normalized_value == normalized,
                    ))
                    if not existing_point:
                        legacy_value = lead.phone if kind == 'phone' else lead.email
                        session.add(ContactPoint(
                            lead_id=lead.id,
                            kind=kind,
                            value=value,
                            normalized_value=normalized,
                            label='Skip trace result',
                            source_name=batch.provider,
                            confidence='unverified',
                            status='active',
                            is_primary=not bool(legacy_value),
                            notes=(
                                'Imported as an alternate; existing primary preserved'
                                if legacy_value else None
                            ),
                            created_at=now,
                            updated_at=now,
                        ))
                        added_point = True
                if conflict:
                    summary['conflicts'] += 1
                    session.add(LeadActivity(
                        lead_id=lead.id,
                        activity_type='enrichment_conflict',
                        detail=(
                            f"Contact conflict from {batch.provider}; "
                            "existing data preserved"
                        ),
                        created_at=now,
                    ))
                    continue
                changed = False
                if phone and not lead.phone:
                    lead.phone = phone
                    changed = True
                if email and not lead.email:
                    lead.email = email
                    changed = True
                if changed:
                    lead.updated_at = now
                    summary['leads_updated'] += 1
                    session.add(LeadActivity(
                        lead_id=lead.id,
                        activity_type='contact_enriched',
                        detail=f"Contact data imported from {batch.provider}",
                        created_at=now,
                    ))
                elif added_point:
                    summary['leads_updated'] += 1
                else:
                    summary['no_data'] += 1
            batch.status = (
                'completed_with_conflicts'
                if summary['conflicts'] else 'completed'
            )
            batch.result_summary_json = json.dumps(summary)
            batch.updated_at = now
        return self.get_enrichment_batch(batch_id)

    def daily_execution(self):
        local_zone = ZoneInfo('America/Chicago')
        local_now = datetime.now(local_zone)
        work_date = local_now.date().isoformat()
        local_start = datetime.combine(
            local_now.date(),
            datetime.min.time(),
            tzinfo=local_zone,
        )
        utc_start = local_start.astimezone(timezone.utc)
        today = work_date
        with self.Session() as session:
            check_in = session.scalar(
                select(DailyCheckIn).where(
                    DailyCheckIn.work_date == work_date
                )
            )
            calls = session.scalar(
                select(func.count()).select_from(CallLog).where(
                    CallLog.created_at >= utc_start
                )
            ) or 0
            research = session.scalar(
                select(func.count()).select_from(ResearchEvidence).where(
                    ResearchEvidence.created_at >= utc_start,
                    ResearchEvidence.retracted_at.is_(None),
                )
            ) or 0
            follow_ups = session.scalar(
                select(func.count()).select_from(Lead).where(
                    Lead.next_follow_up.is_not(None),
                    Lead.next_follow_up <= today,
                    Lead.status.not_in(('closed', 'disqualified')),
                )
            ) or 0
        item = _as_dict(check_in) if check_in else None
        if item:
            item['call_progress'] = {
                'completed': int(calls),
                'target': item['call_target'],
                'percent': min(
                    round(calls / max(item['call_target'], 1) * 100),
                    100,
                ),
            }
            item['research_progress'] = {
                'completed': int(research),
                'target': item['research_target'],
                'percent': min(
                    round(research / max(item['research_target'], 1) * 100),
                    100,
                ),
            }
        return {
            'work_date': work_date,
            'timezone': 'America/Chicago',
            'check_in': item,
            'calls_completed': int(calls),
            'research_completed': int(research),
            'follow_ups_due': int(follow_ups),
        }

    def start_daily_check_in(self, payload):
        focus = str(payload.get('focus', '')).strip()
        if not focus:
            raise ValueError('Today’s focus is required')
        try:
            call_target = int(payload.get('call_target', 0))
            research_target = int(payload.get('research_target', 0))
        except (TypeError, ValueError) as error:
            raise ValueError('Daily targets must be whole numbers') from error
        if not 0 <= call_target <= 500 or not 0 <= research_target <= 500:
            raise ValueError('Daily targets must be between 0 and 500')
        work_date = datetime.now(ZoneInfo('America/Chicago')).date().isoformat()
        now = utc_now()
        with self.Session.begin() as session:
            item = session.scalar(
                select(DailyCheckIn).where(
                    DailyCheckIn.work_date == work_date
                )
            )
            if item:
                raise ValueError('Today’s check-in already exists')
            session.add(DailyCheckIn(
                work_date=work_date,
                focus=focus,
                call_target=call_target,
                research_target=research_target,
                status='open',
                created_at=now,
            ))
        return self.daily_execution()

    def close_daily_check_in(self, closing_notes):
        work_date = datetime.now(ZoneInfo('America/Chicago')).date().isoformat()
        notes = str(closing_notes or '').strip() or None
        now = utc_now()
        with self.Session.begin() as session:
            item = session.scalar(
                select(DailyCheckIn).where(
                    DailyCheckIn.work_date == work_date
                )
            )
            if not item:
                return None
            if item.status != 'completed':
                item.status = 'completed'
                item.closing_notes = notes
                item.completed_at = now
        return self.daily_execution()

    def sync_operational_alerts(self):
        now = utc_now()
        today = date.today().isoformat()
        candidates = []
        with self.Session() as session:
            overdue = session.scalars(
                select(Lead).where(
                    Lead.next_follow_up.is_not(None),
                    Lead.next_follow_up < today,
                    Lead.status.not_in(('closed', 'disqualified')),
                )
            ).all()
            evidence = session.scalars(
                select(ResearchEvidence).where(
                    ResearchEvidence.retracted_at.is_(None)
                )
            ).all()
            jobs = session.scalars(select(ProcessingJob)).all()
            batches = session.scalars(
                select(EnrichmentBatch).where(
                    EnrichmentBatch.status == 'completed_with_conflicts'
                )
            ).all()

        for lead in overdue:
            candidates.append({
                'fingerprint': f"followup:{lead.id}:{lead.next_follow_up}",
                'alert_type': 'overdue_follow_up',
                'severity': 'urgent',
                'title': f"Follow-up overdue · {lead.owner_name}",
                'detail': (
                    f"Due {lead.next_follow_up} for "
                    f"{lead.property_address or lead.tax_id or 'this lead'}."
                ),
                'href': f"/leads?lead={lead.id}",
            })

        by_lead = {}
        for item in evidence:
            by_lead.setdefault(item.lead_id, []).append(item)
        for lead_id, items in by_lead.items():
            if summarize_evidence(items)['status'] == 'conflicting_evidence':
                candidates.append({
                    'fingerprint': f"evidence-conflict:{lead_id}",
                    'alert_type': 'evidence_conflict',
                    'severity': 'high',
                    'title': 'Conflicting deceased-owner evidence',
                    'detail': (
                        'Confirmed living and death evidence require manual '
                        'identity resolution before outreach.'
                    ),
                    'href': f"/leads?lead={lead_id}",
                })

        for job in jobs:
            meta = json.loads(job.meta_json)
            status = meta.get('status')
            if status == 'ready_for_approval':
                candidates.append({
                    'fingerprint': f"job-ready:{job.job_id}",
                    'alert_type': 'import_ready',
                    'severity': 'high',
                    'title': 'County list ready for approval',
                    'detail': (
                        f"{meta.get('source_filename', 'Processing run')} "
                        'finished verification and awaits explicit CRM import.'
                    ),
                    'href': f"/?resume={job.job_id}",
                })
            elif status == 'failed':
                candidates.append({
                    'fingerprint': f"job-failed:{job.job_id}",
                    'alert_type': 'processing_failed',
                    'severity': 'urgent',
                    'title': 'County-list processing needs attention',
                    'detail': str(
                        meta.get('error') or 'The processing run failed.'
                    ),
                    'href': f"/?resume={job.job_id}",
                })

        for batch in batches:
            result = json.loads(batch.result_summary_json or '{}')
            candidates.append({
                'fingerprint': f"enrichment-conflict:{batch.batch_id}",
                'alert_type': 'enrichment_conflict',
                'severity': 'high',
                'title': 'Enrichment conflicts need review',
                'detail': (
                    f"{result.get('conflicts', 0)} returned contacts conflict "
                    f"with trusted CRM data in {batch.batch_id}."
                ),
                'href': '/enrichment',
            })

        with self.Session.begin() as session:
            existing = set(session.scalars(
                select(OperationalAlert.fingerprint).where(
                    OperationalAlert.fingerprint.in_(
                        [item['fingerprint'] for item in candidates]
                    )
                )
            ).all()) if candidates else set()
            for item in candidates:
                if item['fingerprint'] not in existing:
                    session.add(OperationalAlert(**item, created_at=now))
        return len(candidates)

    def list_operational_alerts(self, include_read=True, limit=100):
        self.sync_operational_alerts()
        conditions = [] if include_read else [OperationalAlert.read_at.is_(None)]
        with self.Session() as session:
            unread = session.scalar(
                select(func.count()).select_from(OperationalAlert).where(
                    OperationalAlert.read_at.is_(None)
                )
            ) or 0
            items = session.scalars(
                select(OperationalAlert)
                .where(*conditions)
                .order_by(
                    OperationalAlert.read_at.is_not(None),
                    OperationalAlert.created_at.desc(),
                )
                .limit(min(max(int(limit), 1), 200))
            ).all()
        return {
            'unread': int(unread),
            'items': [_as_dict(item) for item in items],
        }

    def mark_operational_alert(self, alert_id):
        with self.Session.begin() as session:
            item = session.get(OperationalAlert, alert_id)
            if not item:
                return None
            if not item.read_at:
                item.read_at = utc_now()
        return _as_dict(item)

    def mark_all_operational_alerts(self):
        now = utc_now()
        with self.Session.begin() as session:
            items = session.scalars(
                select(OperationalAlert).where(
                    OperationalAlert.read_at.is_(None)
                )
            ).all()
            for item in items:
                item.read_at = now
        return len(items)

    def pending_email_alerts(self, limit=50):
        self.sync_operational_alerts()
        with self.Session() as session:
            items = session.scalars(
                select(OperationalAlert)
                .where(
                    OperationalAlert.read_at.is_(None),
                    OperationalAlert.emailed_at.is_(None),
                )
                .order_by(OperationalAlert.created_at.asc())
                .limit(min(max(int(limit), 1), 100))
            ).all()
            return [_as_dict(item) for item in items]

    def mark_alerts_emailed(self, alert_ids):
        alert_ids = sorted({int(item) for item in alert_ids})
        if not alert_ids:
            return 0
        now = utc_now()
        with self.Session.begin() as session:
            items = session.scalars(
                select(OperationalAlert).where(
                    OperationalAlert.id.in_(alert_ids),
                    OperationalAlert.emailed_at.is_(None),
                )
            ).all()
            for item in items:
                item.emailed_at = now
        return len(items)

    def pending_sms_alerts(self, limit=20):
        self.sync_operational_alerts()
        with self.Session() as session:
            items = session.scalars(
                select(OperationalAlert)
                .where(
                    OperationalAlert.read_at.is_(None),
                    OperationalAlert.sms_sent_at.is_(None),
                    OperationalAlert.severity == 'urgent',
                )
                .order_by(OperationalAlert.created_at.asc())
                .limit(min(max(int(limit), 1), 50))
            ).all()
            return [_as_dict(item) for item in items]

    def mark_alerts_sms_sent(self, alert_ids):
        alert_ids = sorted({int(item) for item in alert_ids})
        if not alert_ids:
            return 0
        now = utc_now()
        with self.Session.begin() as session:
            items = session.scalars(
                select(OperationalAlert).where(
                    OperationalAlert.id.in_(alert_ids),
                    OperationalAlert.sms_sent_at.is_(None),
                )
            ).all()
            for item in items:
                item.sms_sent_at = now
        return len(items)
