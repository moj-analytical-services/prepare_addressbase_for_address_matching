"""Unit tests for chunking functionality."""

from __future__ import annotations

import pytest

from abp_pipeline.transform.common import chunk_where


class TestChunkWhere:
    """Tests for the chunk_where SQL predicate generator."""

    def test_valid_single_chunk(self) -> None:
        """Test chunk_where with num_chunks=1."""
        result = chunk_where("uprn", num_chunks=1, chunk_id=0)
        assert "uprn IS NOT NULL" in result
        assert "(hash(uprn) % 1) = 0" in result

    def test_valid_multiple_chunks(self) -> None:
        """Test chunk_where with num_chunks > 1."""
        result = chunk_where("uprn", num_chunks=10, chunk_id=3)
        assert "uprn IS NOT NULL" in result
        assert "(hash(uprn) % 10) = 3" in result

    def test_different_column_name(self) -> None:
        """Test chunk_where with different column name."""
        result = chunk_where("my_column", num_chunks=5, chunk_id=2)
        assert "my_column IS NOT NULL" in result
        assert "(hash(my_column) % 5) = 2" in result

    def test_invalid_num_chunks_zero(self) -> None:
        """Test that num_chunks=0 raises ValueError."""
        with pytest.raises(ValueError, match="num_chunks must be >= 1"):
            chunk_where("uprn", num_chunks=0, chunk_id=0)

    def test_invalid_num_chunks_negative(self) -> None:
        """Test that negative num_chunks raises ValueError."""
        with pytest.raises(ValueError, match="num_chunks must be >= 1"):
            chunk_where("uprn", num_chunks=-1, chunk_id=0)

    def test_invalid_chunk_id_negative(self) -> None:
        """Test that negative chunk_id raises ValueError."""
        with pytest.raises(ValueError, match="chunk_id must be in range"):
            chunk_where("uprn", num_chunks=5, chunk_id=-1)

    def test_invalid_chunk_id_too_large(self) -> None:
        """Test that chunk_id >= num_chunks raises ValueError."""
        with pytest.raises(ValueError, match="chunk_id must be in range"):
            chunk_where("uprn", num_chunks=5, chunk_id=5)

    def test_invalid_chunk_id_way_too_large(self) -> None:
        """Test that chunk_id much larger than num_chunks raises ValueError."""
        with pytest.raises(ValueError, match="chunk_id must be in range"):
            chunk_where("uprn", num_chunks=5, chunk_id=100)

    def test_boundary_chunk_id(self) -> None:
        """Test chunk_id at boundary (num_chunks - 1)."""
        result = chunk_where("uprn", num_chunks=5, chunk_id=4)
        assert "(hash(uprn) % 5) = 4" in result

    def test_first_chunk_id(self) -> None:
        """Test chunk_id = 0 (first chunk)."""
        result = chunk_where("uprn", num_chunks=5, chunk_id=0)
        assert "(hash(uprn) % 5) = 0" in result
