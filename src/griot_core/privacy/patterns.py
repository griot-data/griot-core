"""
PII Pattern definitions for privacy detection.

This module provides PIIPattern dataclass and pattern collections for
different regions. Patterns are utility data that can be:
1. Bundled into the pii-detection WASM executor
2. Passed as parameters at runtime
3. Used by orchestrator integrations

Note: Validation functions (luhn_check, iban_check, etc.) are kept
as pure functions with no external dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional

from griot_core.models.enums import ComplianceFramework, PIIType


@dataclass
class PIIPattern:
    """
    Pattern definition for PII detection.

    Used to detect and classify personally identifiable information
    in data columns using regex patterns and optional validators.

    Attributes:
        name: Human-readable name for this pattern (e.g., "Kenya National ID")
        pii_type: The type of PII this pattern detects
        pattern: Regular expression pattern for detection
        description: Description of what this pattern matches
        confidence: Confidence level when pattern matches (0.0 to 1.0)
        validator: Optional validation function for additional verification
        frameworks: Compliance frameworks this pattern is relevant to
        region: Geographic region for this pattern (e.g., "kenya", "eu", "universal")
    """

    name: str
    pii_type: PIIType
    pattern: str  # Regex pattern
    description: str
    confidence: float = 0.9
    validator: Optional[Callable[[str], bool]] = None
    frameworks: Optional[List[ComplianceFramework]] = None
    region: str = "universal"


# =============================================================================
# Validation Functions (Pure, No Dependencies)
# =============================================================================


def luhn_check(card_number: str) -> bool:
    """
    Validate a credit card number using the Luhn algorithm.

    Args:
        card_number: The card number to validate (digits only)

    Returns:
        True if the card number is valid per Luhn algorithm
    """
    digits = [int(d) for d in card_number if d.isdigit()]
    if len(digits) < 13:
        return False

    # Double every second digit from right
    for i in range(len(digits) - 2, -1, -2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9

    return sum(digits) % 10 == 0


def iban_check(iban: str) -> bool:
    """
    Validate an IBAN using mod 97 algorithm.

    Args:
        iban: The IBAN to validate

    Returns:
        True if the IBAN checksum is valid
    """
    # Remove spaces and convert to uppercase
    iban = iban.replace(" ", "").upper()

    if len(iban) < 4:
        return False

    # Move first 4 characters to end
    rearranged = iban[4:] + iban[:4]

    # Convert letters to numbers (A=10, B=11, ..., Z=35)
    numeric = ""
    for char in rearranged:
        if char.isdigit():
            numeric += char
        elif char.isalpha():
            numeric += str(ord(char) - ord("A") + 10)
        else:
            return False

    # Check mod 97
    return int(numeric) % 97 == 1


def kenya_id_check(national_id: str) -> bool:
    """
    Validate a Kenya National ID number format.

    Args:
        national_id: The national ID to validate

    Returns:
        True if the format is valid (7-8 digits)
    """
    digits = "".join(c for c in national_id if c.isdigit())
    return 7 <= len(digits) <= 8


def kra_pin_check(kra_pin: str) -> bool:
    """
    Validate a Kenya Revenue Authority PIN format.

    Format: A followed by 9 digits followed by a letter (e.g., A123456789B)

    Args:
        kra_pin: The KRA PIN to validate

    Returns:
        True if the format is valid
    """
    kra_pin = kra_pin.strip().upper()
    if len(kra_pin) != 11:
        return False

    return kra_pin[0].isalpha() and kra_pin[1:10].isdigit() and kra_pin[10].isalpha()


def email_format_check(email: str) -> bool:
    """
    Basic email format validation.

    Args:
        email: The email address to validate

    Returns:
        True if the format appears valid
    """
    import re

    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


# =============================================================================
# Kenya Patterns
# =============================================================================

KENYA_PATTERNS: List[PIIPattern] = [
    PIIPattern(
        name="Kenya National ID",
        pii_type=PIIType.NATIONAL_ID,
        pattern=r"\b\d{7,8}\b",
        description="Kenya National ID number (7-8 digits)",
        confidence=0.7,  # Lower confidence due to simple pattern
        validator=kenya_id_check,
        frameworks=[ComplianceFramework.KENYA_DPA],
        region="kenya",
    ),
    PIIPattern(
        name="KRA PIN",
        pii_type=PIIType.TAX_ID,
        pattern=r"\b[A-Z]\d{9}[A-Z]\b",
        description="Kenya Revenue Authority PIN",
        confidence=0.95,
        validator=kra_pin_check,
        frameworks=[ComplianceFramework.KENYA_DPA],
        region="kenya",
    ),
    PIIPattern(
        name="Kenya Phone Number",
        pii_type=PIIType.PHONE,
        pattern=r"\b(?:\+254|254|0)[17]\d{8}\b",
        description="Kenya mobile phone number",
        confidence=0.85,
        frameworks=[ComplianceFramework.KENYA_DPA],
        region="kenya",
    ),
    PIIPattern(
        name="M-PESA Transaction ID",
        pii_type=PIIType.FINANCIAL,
        pattern=r"\b[A-Z]{2,3}\d{8,10}[A-Z]{0,2}\b",
        description="M-PESA transaction reference",
        confidence=0.8,
        frameworks=[ComplianceFramework.KENYA_DPA],
        region="kenya",
    ),
]


# =============================================================================
# EU Patterns
# =============================================================================

EU_PATTERNS: List[PIIPattern] = [
    PIIPattern(
        name="EU IBAN",
        pii_type=PIIType.IBAN,
        pattern=r"\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b",
        description="International Bank Account Number",
        confidence=0.9,
        validator=iban_check,
        frameworks=[ComplianceFramework.GDPR, ComplianceFramework.PCI_DSS],
        region="eu",
    ),
    PIIPattern(
        name="EU VAT Number",
        pii_type=PIIType.TAX_ID,
        pattern=r"\b[A-Z]{2}\d{8,12}\b",
        description="EU VAT registration number",
        confidence=0.85,
        frameworks=[ComplianceFramework.GDPR],
        region="eu",
    ),
    PIIPattern(
        name="German Personal ID",
        pii_type=PIIType.NATIONAL_ID,
        pattern=r"\b[CFGHJKLMNPRTVWXYZ0-9]{9}D?<<\d{6,7}[MF<]\d{7}\b",
        description="German Personalausweis (ID card) number",
        confidence=0.9,
        frameworks=[ComplianceFramework.GDPR],
        region="eu",
    ),
]


# =============================================================================
# Universal Patterns
# =============================================================================

UNIVERSAL_PATTERNS: List[PIIPattern] = [
    PIIPattern(
        name="Email Address",
        pii_type=PIIType.EMAIL,
        pattern=r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b",
        description="Email address",
        confidence=0.95,
        validator=email_format_check,
        frameworks=[
            ComplianceFramework.GDPR,
            ComplianceFramework.CCPA,
            ComplianceFramework.KENYA_DPA,
        ],
        region="universal",
    ),
    PIIPattern(
        name="Credit Card Number",
        pii_type=PIIType.CREDIT_CARD,
        pattern=r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b",
        description="Credit card number (Visa, Mastercard, Amex, Discover)",
        confidence=0.9,
        validator=luhn_check,
        frameworks=[
            ComplianceFramework.PCI_DSS,
            ComplianceFramework.GDPR,
            ComplianceFramework.CCPA,
        ],
        region="universal",
    ),
    PIIPattern(
        name="IPv4 Address",
        pii_type=PIIType.IP_ADDRESS,
        pattern=r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b",
        description="IPv4 address",
        confidence=0.95,
        frameworks=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
        region="universal",
    ),
    PIIPattern(
        name="MAC Address",
        pii_type=PIIType.MAC_ADDRESS,
        pattern=r"\b(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}\b",
        description="MAC address",
        confidence=0.95,
        frameworks=[ComplianceFramework.GDPR],
        region="universal",
    ),
    PIIPattern(
        name="US Social Security Number",
        pii_type=PIIType.SSN,
        pattern=r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
        description="US Social Security Number",
        confidence=0.85,
        frameworks=[ComplianceFramework.CCPA, ComplianceFramework.HIPAA],
        region="universal",
    ),
    PIIPattern(
        name="Date of Birth",
        pii_type=PIIType.DATE_OF_BIRTH,
        pattern=r"\b(?:19|20)\d{2}[-/](?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12]\d|3[01])\b",
        description="Date of birth (YYYY-MM-DD or YYYY/MM/DD)",
        confidence=0.7,  # Lower confidence - could be any date
        frameworks=[
            ComplianceFramework.GDPR,
            ComplianceFramework.HIPAA,
            ComplianceFramework.KENYA_DPA,
        ],
        region="universal",
    ),
    PIIPattern(
        name="Passport Number",
        pii_type=PIIType.PASSPORT,
        pattern=r"\b[A-Z]{1,2}[0-9]{6,9}\b",
        description="Passport number (generic format)",
        confidence=0.7,
        frameworks=[ComplianceFramework.GDPR, ComplianceFramework.KENYA_DPA],
        region="universal",
    ),
]


# =============================================================================
# Combined Pattern Sets
# =============================================================================


def get_patterns_for_region(region: str) -> List[PIIPattern]:
    """
    Get all PII patterns for a specific region.

    Args:
        region: The region code ("kenya", "eu", "universal", or "all")

    Returns:
        List of PIIPattern objects for the specified region
    """
    if region == "kenya":
        return KENYA_PATTERNS + UNIVERSAL_PATTERNS
    elif region == "eu":
        return EU_PATTERNS + UNIVERSAL_PATTERNS
    elif region == "universal":
        return UNIVERSAL_PATTERNS
    elif region == "all":
        return KENYA_PATTERNS + EU_PATTERNS + UNIVERSAL_PATTERNS
    else:
        return UNIVERSAL_PATTERNS


def get_patterns_for_framework(framework: ComplianceFramework) -> List[PIIPattern]:
    """
    Get all PII patterns relevant to a compliance framework.

    Args:
        framework: The compliance framework

    Returns:
        List of PIIPattern objects relevant to the framework
    """
    all_patterns = KENYA_PATTERNS + EU_PATTERNS + UNIVERSAL_PATTERNS
    return [p for p in all_patterns if p.frameworks and framework in p.frameworks]
