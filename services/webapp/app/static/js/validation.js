/**
 * validation.js - Schema-driven form validation using Pydantic-generated JSON Schema.
 * 
 * Uses Ajv (Another JSON Schema Validator) for client-side validation.
 * Fetches schema from backend endpoint and validates form data before submission.
 * 
 * Usage:
 *   const validator = new ManifestFormValidator('manifest-form', 'spatial');
 *   // Form will be validated on submit, errors displayed inline
 */

/**
 * ManifestFormValidator - Client-side form validation using JSON Schema
 */
class ManifestFormValidator {
    /**
     * Create a new form validator
     * @param {string} formId - ID of the form element
     * @param {string} assetType - Asset type (spatial, tabular, joined)
     * @param {Object} options - Optional configuration
     * @param {Function} options.onValid - Callback when form is valid
     * @param {Function} options.onInvalid - Callback when form is invalid
     * @param {Function} options.collectData - Custom data collection function
     */
    constructor(formId, assetType, options = {}) {
        this.form = document.getElementById(formId);
        this.assetType = assetType;
        this.schema = null;
        this.ajv = null;
        this.validate = null;
        this.options = options;
        
        if (!this.form) {
            console.error(`ManifestFormValidator: Form not found: ${formId}`);
            return;
        }
        
        this.init();
    }
    
    /**
     * Initialize the validator - fetch schema and set up event listeners
     */
    async init() {
        try {
            // Check if Ajv is loaded
            if (typeof Ajv === 'undefined') {
                console.warn('ManifestFormValidator: Ajv not loaded, validation disabled');
                return;
            }
            
            // Initialize Ajv with options
            this.ajv = new Ajv({ 
                allErrors: true, 
                verbose: true,
                strict: false,  // Allow Pydantic-specific keywords
                validateFormats: false  // Don't validate format strings
            });
            
            // Fetch schema from backend
            const response = await fetch(`/manifests/schemas/${this.assetType}`);
            if (!response.ok) {
                throw new Error(`Failed to fetch schema: ${response.status}`);
            }
            this.schema = await response.json();
            
            // Compile the schema
            this.validate = this.ajv.compile(this.schema);
            
            // Attach validation to form submit
            this.form.addEventListener('submit', (e) => this.handleSubmit(e));
            
            // Optional: Real-time validation on blur
            this.form.querySelectorAll('input, select, textarea').forEach(field => {
                field.addEventListener('blur', () => this.validateField(field.name));
            });
            
            console.log(`ManifestFormValidator: Initialized for ${this.assetType}`);
        } catch (error) {
            console.error('ManifestFormValidator: Initialization failed', error);
        }
    }
    
    /**
     * Handle form submission - validate and prevent if invalid
     * @param {Event} event - Submit event
     */
    handleSubmit(event) {
        // If no schema loaded, allow native validation
        if (!this.validate) {
            return true;
        }
        
        // Clear previous errors
        this.clearErrors();
        
        // Collect form data
        const data = this.collectFormData();
        
        // Validate against schema
        const valid = this.validate(data);
        
        if (!valid) {
            event.preventDefault();
            this.displayErrors(this.validate.errors);
            
            if (this.options.onInvalid) {
                this.options.onInvalid(this.validate.errors, data);
            }
            
            return false;
        }
        
        if (this.options.onValid) {
            this.options.onValid(data);
        }
        
        return true;
    }
    
    /**
     * Collect form data and transform to match expected schema structure
     * @returns {Object} Form data object
     */
    collectFormData() {
        // Allow custom data collection
        if (this.options.collectData) {
            return this.options.collectData(this.form);
        }
        
        const formData = new FormData(this.form);
        const data = {};
        
        for (const [key, value] of formData.entries()) {
            // Handle special cases
            if (key === 'keywords' && value) {
                // Parse comma-separated keywords to array
                data[key] = value.split(',').map(k => k.trim()).filter(Boolean);
            } else if (key === 'tag_key' || key === 'tag_value') {
                // Skip - handled by collectTags
                continue;
            } else if (key === 'file_path' || key === 'file_format' || key === 'file_type') {
                // Skip - handled by collectFiles
                continue;
            } else if (value === '' || value === null) {
                // Convert empty strings to null for optional fields
                data[key] = null;
            } else {
                data[key] = value;
            }
        }
        
        // Collect files if collectFiles is available
        if (typeof collectFiles === 'function') {
            data.files = collectFiles();
        }
        
        // Collect tags if collectTags is available
        if (typeof collectTags === 'function') {
            data.tags = collectTags();
        }
        
        return data;
    }
    
    /**
     * Validate a single field (for real-time feedback)
     * @param {string} fieldName - Name of field to validate
     */
    validateField(fieldName) {
        if (!this.validate) return;
        
        const data = this.collectFormData();
        const valid = this.validate(data);
        
        // Clear existing error for this field
        this.clearFieldError(fieldName);
        
        if (!valid) {
            // Find errors for this specific field
            const fieldErrors = this.validate.errors.filter(error => {
                const errorField = error.instancePath.replace('/', '') || 
                                   error.params?.missingProperty;
                return errorField === fieldName;
            });
            
            if (fieldErrors.length > 0) {
                this.displayFieldError(fieldName, fieldErrors[0]);
            } else {
                // Mark as validated if no errors for this field
                const input = this.form.querySelector(`[name="${fieldName}"]`);
                if (input) {
                    input.classList.add('validated');
                }
            }
        } else {
            // Mark as validated
            const input = this.form.querySelector(`[name="${fieldName}"]`);
            if (input) {
                input.classList.add('validated');
            }
        }
    }
    
    /**
     * Display all validation errors
     * @param {Array} errors - Array of Ajv error objects
     */
    displayErrors(errors) {
        errors.forEach(error => {
            const fieldName = error.instancePath.replace('/', '') || 
                              error.params?.missingProperty;
            
            if (fieldName) {
                this.displayFieldError(fieldName, error);
            }
        });
        
        // Scroll to first error
        const firstError = this.form.querySelector('.field-error:not(:empty)');
        if (firstError) {
            firstError.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
    
    /**
     * Display error for a specific field
     * @param {string} fieldName - Field name
     * @param {Object} error - Ajv error object
     */
    displayFieldError(fieldName, error) {
        const errorEl = document.getElementById(`${fieldName}-error`);
        if (errorEl) {
            errorEl.textContent = this.formatErrorMessage(error);
            errorEl.style.display = 'block';
        }
        
        // Add error class to input
        const input = this.form.querySelector(`[name="${fieldName}"]`);
        if (input) {
            input.classList.remove('validated');
            input.setAttribute('aria-invalid', 'true');
        }
    }
    
    /**
     * Format an Ajv error into a user-friendly message
     * @param {Object} error - Ajv error object
     * @returns {string} Human-readable error message
     */
    formatErrorMessage(error) {
        switch (error.keyword) {
            case 'required':
                return `This field is required`;
            case 'minLength':
                return `Must be at least ${error.params.limit} characters`;
            case 'maxLength':
                return `Must be at most ${error.params.limit} characters`;
            case 'pattern':
                return `Invalid format`;
            case 'enum':
                return `Must be one of: ${error.params.allowedValues.join(', ')}`;
            case 'type':
                return `Must be a ${error.params.type}`;
            case 'format':
                return `Invalid ${error.params.format} format`;
            default:
                return error.message || 'Invalid value';
        }
    }
    
    /**
     * Clear error for a specific field
     * @param {string} fieldName - Field name
     */
    clearFieldError(fieldName) {
        const errorEl = document.getElementById(`${fieldName}-error`);
        if (errorEl) {
            errorEl.textContent = '';
            errorEl.style.display = 'none';
        }
        
        const input = this.form.querySelector(`[name="${fieldName}"]`);
        if (input) {
            input.removeAttribute('aria-invalid');
        }
    }
    
    /**
     * Clear all validation errors
     */
    clearErrors() {
        this.form.querySelectorAll('.field-error').forEach(el => {
            el.textContent = '';
            el.style.display = 'none';
        });
        
        this.form.querySelectorAll('[aria-invalid]').forEach(el => {
            el.removeAttribute('aria-invalid');
        });
    }
}

// Export for module usage (if using modules)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { ManifestFormValidator };
}
