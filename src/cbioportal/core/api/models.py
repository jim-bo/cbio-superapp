"""Pydantic v2 models for cBioPortal REST API responses."""
from __future__ import annotations

from pydantic import BaseModel


class CancerType(BaseModel):
    cancerTypeId: str
    name: str


class Study(BaseModel):
    studyId: str
    name: str
    description: str | None = None
    cancerType: CancerType | None = None
    allSampleCount: int = 0


class Mutation(BaseModel):
    hugoGeneSymbol: str
    proteinChange: str | None = None
    mutationType: str | None = None
    chr: str | None = None
    startPosition: int | None = None
    endPosition: int | None = None
    referenceAllele: str | None = None
    variantAllele: str | None = None
    tumorSampleBarcode: str
    # annotation fields (None until annotated)
    oncogenic: str | None = None
    mutationEffect: str | None = None
    highestLevel: str | None = None
    therapeuticSensitivity: str | None = None


class CnaSegment(BaseModel):
    sampleId: str
    chromosome: str
    start: int
    end: int
    numberOfProbes: int | None = None
    segmentMean: float


class ClinicalRow(BaseModel):
    sampleId: str
    patientId: str
    studyId: str
    attributes: dict[str, str] = {}
