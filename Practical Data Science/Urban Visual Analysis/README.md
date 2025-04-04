# Urban Visual Analysis

## Project Overview
Urban Visual Analysis is a **data-driven approach** to understanding cityscapes through **image-based feature extraction** and **clustering techniques**. By analyzing the **visual characteristics** of urban images, this project aims to identify **patterns, similarities, and distinctions** between different cities.

## Key Features
- **Feature Extraction**: Extracts key visual characteristics from city images.
- **Clustering of City Images**: Groups similar images using **K-Means clustering**.
- **Annotator Agreement Visualization**: Measures agreement levels among multiple annotators.
- **Statistical & AI-Based Analysis**: Employs **PCA, Cosine Similarity, and Silhouette Score** to evaluate clustering quality.

## Feature Analysis
### 1️⃣ **Color-Based Features**
- **Average RGB Color Abundance**: Computes the mean **Red, Green, and Blue intensity** for each city.
- **Dominant Color Extraction**: Uses **K-Means clustering (k=7)** to identify dominant color clusters.

### 2️⃣ **Texture & Structural Features**
- **Brightness**: Converts images to grayscale and computes mean pixel intensity.
- **Contrast, Dissimilarity, Homogeneity, Energy, Correlation**: Extracted using the **Gray-Level Co-occurrence Matrix (GLCM)**.
- **Sharpness**: Uses **Laplacian Operator** to detect image sharpness.
- **Edge Density**: Computes the complexity of the image using **Canny Edge Detection**.
- **Local Binary Pattern (LBP)**: Captures local texture patterns.
- **Fractal Dimension**: Measures complexity and self-similarity of urban textures.

### 3️⃣ **Shape & Structural Complexity**
- **Shape Features**: Detects contours and quantifies geometric properties of objects.
- **Spatial Center of Mass**: Computes symmetry and object localization.

### 4️⃣ **Clustering & Statistical Analysis**
- **K-Means Clustering**:
  - **n_init=10**: Runs with different initializations.
  - **max_iter=4000**: Maximum iterations for convergence.
  - **tol=1e-5**: Tolerance for convergence.
- **Silhouette Score**: Evaluates cluster quality.
- **Principal Component Analysis (PCA)**: Reduces feature dimensions while retaining variance.
- **Cosine Similarity**: Measures city similarity based on image clustering.

### 5️⃣ **Annotator Agreement Analysis**
- **Cohen’s Kappa**: Evaluates agreement among annotators.

