/**
 * forms.js - Dynamic form handling for manifest creation
 */

// Add file entry button (for spatial forms)
document.addEventListener('DOMContentLoaded', function () {
    const addFileBtn = document.getElementById('add-file-btn');
    if (addFileBtn) {
        addFileBtn.addEventListener('click', function () {
            const container = document.getElementById('files-container');
            const newEntry = document.createElement('div');
            newEntry.className = 'file-entry';
            newEntry.innerHTML = `
                <hr>
                <label>
                    File Path
                    <input type="text" name="file_path" placeholder="s3://landing-zone/...">
                </label>
                <div class="grid">
                    <label>
                        Format
                        <select name="file_format">
                            <option value="GeoJSON">GeoJSON</option>
                            <option value="SHP">Shapefile</option>
                            <option value="GPKG">GeoPackage</option>
                            <option value="GTiff">GeoTIFF</option>
                        </select>
                    </label>
                    <label>
                        Type
                        <select name="file_type">
                            <option value="vector">Vector</option>
                            <option value="raster">Raster</option>
                        </select>
                    </label>
                    <button type="button" class="remove-file-btn secondary outline">Remove</button>
                </div>
            `;
            container.appendChild(newEntry);

            // Add remove listener
            newEntry.querySelector('.remove-file-btn').addEventListener('click', function () {
                newEntry.remove();
            });
        });
    }

    // Add tag entry button
    const addTagBtn = document.getElementById('add-tag-btn');
    if (addTagBtn) {
        addTagBtn.addEventListener('click', function () {
            const container = document.getElementById('tags-container');
            const newEntry = document.createElement('div');
            newEntry.className = 'tag-entry grid';
            newEntry.innerHTML = `
                <input type="text" name="tag_key" placeholder="Key">
                <input type="text" name="tag_value" placeholder="Value">
                <button type="button" class="remove-tag-btn secondary outline" style="max-width: 100px;">Remove</button>
            `;
            container.appendChild(newEntry);

            // Add remove listener
            newEntry.querySelector('.remove-tag-btn').addEventListener('click', function () {
                newEntry.remove();
            });
        });
    }
});

/**
 * Collect all file entries from the form
 */
function collectFiles() {
    const paths = document.querySelectorAll('input[name="file_path"]');
    const formats = document.querySelectorAll('select[name="file_format"]');
    const types = document.querySelectorAll('select[name="file_type"]');

    const files = [];
    paths.forEach((pathInput, i) => {
        if (pathInput.value) {
            files.push({
                path: pathInput.value,
                type: types[i] ? types[i].value : 'vector',
                format: formats[i] ? formats[i].value : 'GeoJSON'
            });
        }
    });

    return files;
}

/**
 * Collect all tag entries from the form
 */
function collectTags() {
    const keys = document.querySelectorAll('input[name="tag_key"]');
    const values = document.querySelectorAll('input[name="tag_value"]');

    const tags = {};
    keys.forEach((keyInput, i) => {
        if (keyInput.value && values[i] && values[i].value) {
            tags[keyInput.value] = values[i].value;
        }
    });

    return tags;
}

/**
 * Format file size for display
 */
function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}
