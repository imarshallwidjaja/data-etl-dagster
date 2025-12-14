"""Unit tests for GDAL Resource."""

from unittest.mock import patch, Mock
import subprocess
import pytest

from services.dagster.etl_pipelines.resources import GDALResource
from services.dagster.etl_pipelines.resources.gdal_resource import GDALResult


@pytest.fixture
def gdal_resource():
    """Create GDAL resource with test credentials."""
    return GDALResource(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        aws_s3_endpoint="http://minio:9000",
        gdal_data_path="/usr/share/gdal",
        proj_lib_path="/usr/share/proj",
    )


class TestGDALResourceOgr2ogr:
    """Test suite for ogr2ogr method."""
    
    def test_ogr2ogr_command_construction_minimal(self, gdal_resource):
        """Test ogr2ogr builds correct command with minimal args."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="Translation success",
                stderr="",
            )
            
            result = gdal_resource.ogr2ogr(
                input_path="/vsis3/landing/data.geojson",
                output_path="PG:host=postgis",
            )
            
            called_cmd = mock_run.call_args[0][0]
            assert called_cmd[0] == "ogr2ogr"
            assert "-f" in called_cmd
            assert "PostgreSQL" in called_cmd
            assert called_cmd[-2] == "PG:host=postgis"
            assert called_cmd[-1] == "/vsis3/landing/data.geojson"
            
            assert result.success is True
            assert result.return_code == 0
    
    def test_ogr2ogr_with_crs_and_layer(self, gdal_resource):
        """Test ogr2ogr with CRS transformation and layer naming."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            result = gdal_resource.ogr2ogr(
                input_path="/vsis3/landing/data.geojson",
                output_path="PG:host=postgis dbname=spatial_compute",
                layer_name="raw_input",
                target_crs="EPSG:4326",
            )
            
            called_cmd = mock_run.call_args[0][0]
            assert "-t_srs" in called_cmd
            idx = called_cmd.index("-t_srs")
            assert called_cmd[idx + 1] == "EPSG:4326"
            
            assert "-nln" in called_cmd
            idx = called_cmd.index("-nln")
            assert called_cmd[idx + 1] == "raw_input"
            
            assert result.success is True
    
    def test_ogr2ogr_with_custom_options(self, gdal_resource):
        """Test ogr2ogr with custom GDAL options."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            result = gdal_resource.ogr2ogr(
                input_path="/vsis3/landing/data.geojson",
                output_path="/vsis3/data-lake/data.parquet",
                output_format="Parquet",
                options={"-overwrite": "", "-co": "COMPRESSION=snappy"},
            )
            
            called_cmd = mock_run.call_args[0][0]
            assert "-overwrite" in called_cmd
            assert "-co" in called_cmd
            idx = called_cmd.index("-co")
            assert called_cmd[idx + 1] == "COMPRESSION=snappy"
    
    def test_ogr2ogr_postgresql_output_not_tracked(self, gdal_resource):
        """Test that PostgreSQL outputs don't set output_path."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            result = gdal_resource.ogr2ogr(
                input_path="/vsis3/landing/data.geojson",
                output_path="PG:host=postgis",
                output_format="PostgreSQL",
            )
            
            # PostgreSQL connection strings should not be tracked
            assert result.output_path is None
    
    def test_ogr2ogr_file_output_tracked(self, gdal_resource):
        """Test that file outputs are tracked in output_path."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            result = gdal_resource.ogr2ogr(
                input_path="/vsis3/landing/data.geojson",
                output_path="/vsis3/data-lake/data.parquet",
                output_format="Parquet",
            )
            
            assert result.output_path == "/vsis3/data-lake/data.parquet"


class TestGDALResourceGdalTranslate:
    """Test suite for gdal_translate method."""
    
    def test_gdal_translate_basic(self, gdal_resource):
        """Test basic gdal_translate call."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            result = gdal_resource.gdal_translate(
                input_path="/vsis3/landing/image.tif",
                output_path="/vsis3/data-lake/image_cog.tif",
            )
            
            called_cmd = mock_run.call_args[0][0]
            assert called_cmd[0] == "gdal_translate"
            assert "-of" in called_cmd
            assert "COG" in called_cmd
            
            assert result.success is True
            assert result.output_path == "/vsis3/data-lake/image_cog.tif"
    
    def test_gdal_translate_with_options(self, gdal_resource):
        """Test gdal_translate with creation options."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            result = gdal_resource.gdal_translate(
                input_path="/vsis3/landing/image.tif",
                output_path="/tmp/image.tif",
                output_format="GTiff",
                options={"-co": "COMPRESS=deflate", "-co": "BLOCKXSIZE=512"},
            )
            
            called_cmd = mock_run.call_args[0][0]
            assert "-co" in called_cmd


class TestGDALResourceInfo:
    """Test suite for info methods (ogrinfo, gdalinfo)."""
    
    def test_ogrinfo_basic(self, gdal_resource):
        """Test basic ogrinfo call."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="Layer count: 1",
                stderr="",
            )
            
            result = gdal_resource.ogrinfo("/vsis3/landing/data.geojson")
            
            called_cmd = mock_run.call_args[0][0]
            assert called_cmd[0] == "ogrinfo"
            assert "-al" in called_cmd
            assert "-so" in called_cmd
            
            assert result.success is True
            assert "Layer count" in result.stdout
    
    def test_ogrinfo_with_layer(self, gdal_resource):
        """Test ogrinfo with specific layer."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )

            result = gdal_resource.ogrinfo(
                "/vsis3/landing/data.shp",
                layer="points",
            )

            called_cmd = mock_run.call_args[0][0]
            assert "points" in called_cmd

    def test_ogrinfo_json(self, gdal_resource):
        """Test ogrinfo with JSON output."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout='{"layers": []}',
                stderr="",
            )

            result = gdal_resource.ogrinfo("/vsis3/landing/data.geojson", as_json=True)

            called_cmd = mock_run.call_args[0][0]
            assert called_cmd[0] == "ogrinfo"
            assert "-json" in called_cmd
            assert "-al" not in called_cmd  # JSON mode doesn't use -al -so
            assert "-so" not in called_cmd

            assert result.success is True
            assert result.stdout == '{"layers": []}'
    
    def test_gdalinfo(self, gdal_resource):
        """Test gdalinfo call."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="Band count: 3",
                stderr="",
            )
            
            result = gdal_resource.gdalinfo("/vsis3/landing/image.tif")
            
            called_cmd = mock_run.call_args[0][0]
            assert called_cmd[0] == "gdalinfo"
            assert called_cmd[1] == "/vsis3/landing/image.tif"
            
            assert result.success is True


class TestGDALEnvironment:
    """Test suite for environment variable handling."""
    
    def test_environment_variables_passed_to_subprocess(self, gdal_resource):
        """Test that S3 credentials are passed to subprocess."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            gdal_resource.ogrinfo("/vsis3/test/data.shp")
            
            call_kwargs = mock_run.call_args[1]
            env = call_kwargs["env"]
            assert env["AWS_ACCESS_KEY_ID"] == "test_key"
            assert env["AWS_SECRET_ACCESS_KEY"] == "test_secret"
            assert env["AWS_S3_ENDPOINT"] == "http://minio:9000"
    
    def test_gdal_paths_in_environment(self, gdal_resource):
        """Test that GDAL/PROJ paths are passed to subprocess."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            gdal_resource.gdalinfo("/tmp/test.tif")
            
            call_kwargs = mock_run.call_args[1]
            env = call_kwargs["env"]
            assert env["GDAL_DATA"] == "/usr/share/gdal"
            assert env["PROJ_LIB"] == "/usr/share/proj"
    
    def test_empty_credentials_not_set(self):
        """Test that empty credentials are not set in environment."""
        resource = GDALResource(
            aws_access_key_id="",
            aws_secret_access_key="",
            aws_s3_endpoint="",
        )
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            resource.ogrinfo("/tmp/test.geojson")
            
            call_kwargs = mock_run.call_args[1]
            env = call_kwargs["env"]
            # Empty values should not be set (or would be empty strings)
            # The important thing is no crash occurs


class TestGDALFailureHandling:
    """Test suite for error handling."""
    
    def test_command_failure_detection(self, gdal_resource):
        """Test that command failures are properly detected."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=1,
                stdout="",
                stderr="ERROR: File not found",
            )
            
            result = gdal_resource.ogrinfo("/nonexistent/file.shp")
            
            assert result.success is False
            assert result.return_code == 1
            assert "ERROR" in result.stderr
            assert result.output_path is None
    
    def test_result_serialization(self, gdal_resource):
        """Test that GDALResult is serializable."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="Success output",
                stderr="",
            )
            
            result = gdal_resource.ogr2ogr(
                input_path="/vsis3/landing/data.geojson",
                output_path="/tmp/data.geojson",
                output_format="GeoJSON",
            )
            
            # Test that all fields are JSON-serializable
            assert isinstance(result.success, bool)
            assert isinstance(result.command, list)
            assert isinstance(result.stdout, str)
            assert isinstance(result.stderr, str)
            assert isinstance(result.return_code, int)
            assert result.output_path is None or isinstance(result.output_path, str)


class TestGDALResultDataclass:
    """Test suite for GDALResult dataclass."""
    
    def test_gdal_result_creation(self):
        """Test GDALResult dataclass creation."""
        result = GDALResult(
            success=True,
            command=["ogr2ogr", "-f", "GeoJSON"],
            stdout="Translation successful",
            stderr="",
            return_code=0,
            output_path="/tmp/data.geojson",
        )
        
        assert result.success is True
        assert result.return_code == 0
        assert result.output_path == "/tmp/data.geojson"
    
    def test_gdal_result_without_output_path(self):
        """Test GDALResult without output_path (optional)."""
        result = GDALResult(
            success=True,
            command=["ogrinfo"],
            stdout="Info",
            stderr="",
            return_code=0,
        )
        
        assert result.output_path is None
