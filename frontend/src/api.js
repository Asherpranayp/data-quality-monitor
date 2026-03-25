import axios from 'axios';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
const API = `${BACKEND_URL}/api`;

const api = axios.create({
  baseURL: API,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Dataset APIs
export const uploadDataset = async (formData) => {
  const response = await api.post('/datasets/upload', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return response.data;
};

export const createDatasetFromJson = async (data) => {
  const response = await api.post('/datasets/json', data);
  return response.data;
};

export const getDatasets = async () => {
  const response = await api.get('/datasets');
  return response.data;
};

export const getDataset = async (id) => {
  const response = await api.get(`/datasets/${id}`);
  return response.data;
};

export const updateDataset = async (id, data) => {
  const response = await api.put(`/datasets/${id}`, data);
  return response.data;
};

export const deleteDataset = async (id) => {
  const response = await api.delete(`/datasets/${id}`);
  return response.data;
};

// Validation APIs
export const triggerValidation = async (datasetId) => {
  const response = await api.post(`/validation/run/${datasetId}`);
  return response.data;
};

export const getValidationJobs = async (limit = 50) => {
  const response = await api.get('/validation/jobs', { params: { limit } });
  return response.data;
};

export const getValidationResults = async (jobId) => {
  const response = await api.get(`/validation/results/${jobId}`);
  return response.data;
};

export const checkReferentialIntegrity = async (data) => {
  const response = await api.post('/validation/referential-integrity', data);
  return response.data;
};

// AI Analysis APIs
export const triggerAIAnalysis = async (jobId) => {
  const response = await api.post(`/ai/analyze/${jobId}`);
  return response.data;
};

// Schedule APIs
export const createSchedule = async (data) => {
  const response = await api.post('/schedules', data);
  return response.data;
};

export const getSchedules = async () => {
  const response = await api.get('/schedules');
  return response.data;
};

export const deleteSchedule = async (id) => {
  const response = await api.delete(`/schedules/${id}`);
  return response.data;
};

// Dashboard APIs
export const getDashboardStats = async () => {
  const response = await api.get('/dashboard/stats');
  return response.data;
};

export const healthCheck = async () => {
  const response = await api.get('/health');
  return response.data;
};

// ============== Relationship APIs ==============
export const createRelationship = async (data) => {
  const response = await api.post('/relationships', data);
  return response.data;
};

export const getRelationships = async () => {
  const response = await api.get('/relationships');
  return response.data;
};

export const getDatasetRelationships = async (datasetId) => {
  const response = await api.get(`/relationships/dataset/${datasetId}`);
  return response.data;
};

export const deleteRelationship = async (id) => {
  const response = await api.delete(`/relationships/${id}`);
  return response.data;
};

// ============== Referential Integrity APIs ==============
export const validateIntegrity = async (jobId) => {
  const response = await api.post(`/validation/integrity/${jobId}`);
  return response.data;
};

export const getIntegrityResults = async (jobId) => {
  const response = await api.get(`/validation/integrity/${jobId}`);
  return response.data;
};

// ============== Lineage APIs ==============
export const createLineage = async (data) => {
  const response = await api.post('/lineage', data);
  return response.data;
};

export const getLineage = async () => {
  const response = await api.get('/lineage');
  return response.data;
};

export const getDatasetLineage = async (datasetId) => {
  const response = await api.get(`/lineage/dataset/${datasetId}`);
  return response.data;
};

export const deleteLineage = async (id) => {
  const response = await api.delete(`/lineage/${id}`);
  return response.data;
};

// ============== Validation History APIs ==============
export const getValidationHistory = async (datasetId, limit = 30) => {
  const response = await api.get(`/history/dataset/${datasetId}`, { params: { limit } });
  return response.data;
};

// ============== Dependency Graph API ==============
export const getDependencyGraph = async () => {
  const response = await api.get('/graph/dependencies');
  return response.data;
};

export default api;
