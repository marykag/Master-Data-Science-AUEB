# Food Hazard Detection Challenge

The **Food Hazard Detection Challenge** involves predicting two key categories, **hazard-category** and **product-category**, based on the provided dataset. The objective is to evaluate and compare machine learning models using both short-text (title) and long-text (text) features for accurate classification. Below is a description of the dataset and analysis tasks.

---

## Dataset Overview

This project performs multi-label classification on text data using machine learning algorithms. It includes:
- **Data preprocessing**: Loading and cleaning text data, feature extraction using TF-IDF.
- **Model Building**: Training multiple classification algorithms (e.g., Logistic Regression, Decision Trees, SVM).
- **Hyperparameter Tuning**: Using GridSearchCV/RandomizedSearchCV to optimize hyperparameters.
- **Model Evaluation**: Evaluating model performance using metrics like precision, recall, F1-score, and AUC-ROC.
- **Model Comparison**: Comparing the results of different models.
- **Visualization**: Plotting evaluation metrics for easy comparison.


The dataset includes the following columns:

| **Column Name**       | **Description**                                                                                           |
|------------------------|-----------------------------------------------------------------------------------------------------------|
| `year`                | The year of the recall notification.                                                                     |
| `month`               | The month of the recall notification.                                                                    |
| `day`                 | The day of the recall notification.                                                                      |
| `country`             | The country issuing the recall.                                                                          |
| `title`               | A short description or title of the recall notification.                                                 |
| `text`                | Detailed information about the recall, including product name, problem description, and quantities.      |
| `hazard-category`     | The general category of the hazard (e.g., *biological*, *chemical*).                                      |
| `product-category`    | The general category of the product (e.g., *meat, egg and dairy products*).                               |
| `hazard`              | The specific hazard associated with the product (e.g., *listeria monocytogenes*).                        |
| `product`             | The specific product involved in the recall (e.g., *smoked sausage*).                                    |

---

## Task Description

### Objective:
The goal is to develop machine learning models that predict the **product-category** and **hazard-category** and the **producy** and **hazard** using:
1. **Short Texts (Title)**: Analyze the predictive power of concise information.
2. **Long Texts (Text)**: Leverage detailed descriptions for enhanced accuracy.

### Steps to Perform:
1. **Data Preprocessing**:
   - Tokenize and preprocess the `title` and `text` columns.
   - Perform stopword removal, lemmatization, or stemming.
   - Use techniques like **TF-IDF** to convert text data into numerical representations.

2. **Model Training**:
   - Train separate models for the `title` and `text` data using various machine learning algorithms.
   - Explore both basic (e.g., Logistic Regression, Naive Bayes) and advanced (e.g., Random Forest, Gradient Boosting) models.

3. **Evaluation**:
   - Measure performance using metrics such as **accuracy**, **precision**, **recall**, and **F1-score**.
   - Compare results to determine which approach (short-text vs. long-text) provides better predictions.

4. **Submission**:
   - Submit predictions to the leaderboard for evaluation.
   - Document the results, including the best-performing model, and report the score and rank.

---

## Exploratory Data Analysis

### Sample Data:
Below is a preview of the dataset:

| Year | Month | Day | Country | Title                          | Text                                                                                                                        | Hazard-Category | Product-Category     | Hazard                 | Product        | 
|------|-------|-----|---------|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------------------|------------------------|----------------|
| 1994 | 1     | 7   | US      | Recall Notification: FSIS-024-94 | Case Number: 024-94; Date Opened: 07/01/1994; Date Closed: 09/22/1994; Recall Class: 1; Press Release: Y; Product: SMOKED CHICKEN SAUSAGE; Problem: BACTERIA; Description: LISTERIA; Total Pounds Recalled: 2,894; Pounds Recovered: 2,894 | Biological     | Meat, egg and dairy  | Listeria monocytogenes | Smoked Sausage |
| 1994 | 3     | 10  | US      | Recall Notification: FSIS-033-94 | Case Number: 033-94; Date Opened: 10/03/1994; Date Closed: 01/19/1995; Recall Class: 1; Press Release: Y; Product: WIENERS; Problem: BACTERIA; Description: LISTERIA; Total Pounds Recalled: 5,500; Pounds Recovered: 4,568              | Biological     | Meat, egg and dairy  | Listeria spp           | Sausage        |

### Key Insights:
- The dataset contains structured fields for date and location, and unstructured text fields (`title`, `text`).
- Labels (`hazard-category`, `product-category`, `hazard`, `product`) are categorical, ideal for classification tasks.
- The `text` column provides significantly more details compared to the `title`, which may result in better predictive performance.

---

## Benchmarking Approaches

1. **Short Text Analysis (Title)**:
   - Focus on extracting features from the `title` column.
   - Use simple & advanced models to test the power of concise recall descriptions.

2. **Long Text Analysis (Text)**:
   - Utilize the detailed information in the `text` column.
   - Apply simple & advanced feature extraction and machine learning techniques to maximize prediction accuracy.


**This approach ensures a comprehensive evaluation of the dataset and provides insights into the predictive power of short vs. long textual descriptions for food hazard detection.**
---
## Libraries and Their Usage

**Essential Libraries for Data Analysis and Machine Learning**

   1. **`pandas`**: A powerful data manipulation and analysis library. It provides data structures such as DataFrames for handling structured data.
   2. **`IPython.display`**: A module that enables displaying rich media outputs such as HTML, images, and data frames within Jupyter notebooks.
   3. **`re`**: A library for working with regular expressions, useful for text processing tasks like pattern matching.

---

**Natural Language Processing (NLP) Tools**  

   1. **`nltk`**: The Natural Language Toolkit, used for working with human language data. It provides tools for tokenization, lemmatization, stemming, and more.
   2. **`stopwords`**: Part of the `nltk.corpus`, contains a list of commonly used words (like 'the', 'and', etc.) that can be removed from text during preprocessing.
   3. **`wordnet` (as `wn`)**: A lexical database for the English language, used for finding synonyms, antonyms, definitions, and semantic relationships between words.
   4. **`word_tokenize`**: A function from `nltk.tokenize` that splits text into individual words (tokens).
   5. **`WordNetLemmatizer`**: Part of `nltk.stem`, used for lemmatizing words based on their meanings, converting words to their base form.

---

**Text Feature Extraction**

   1. **`sklearn.feature_extraction.text.TfidfVectorizer`**: A tool for converting text data into numerical feature vectors, using the Term Frequency-Inverse Document Frequency method, commonly used in text classification.

---

**Machine Learning Libraries and Models**

   1. **`sklearn.linear_model.LogisticRegression`**: A model for binary and multiclass classification based on the logistic regression algorithm.
   2. **`sklearn.tree.DecisionTreeClassifier`**: A model that uses decision trees for classification, splitting data based on feature values.
   3. **`sklearn.naive_bayes.MultinomialNB`**: A Naive Bayes classifier that works well for text classification problems.
   4. **`sklearn.ensemble.RandomForestClassifier`**: A model based on an ensemble of decision trees, designed to improve classification performance.
   5. **`sklearn.svm.SVC`**: A Support Vector Classifier for classification tasks, particularly effective for high-dimensional data.
   6. **`sklearn.neural_network.MLPClassifier`**: A multi-layer perceptron classifier, a type of neural network for classification tasks.

 ---

**Utility Modules for Model Training, Tuning, and Preprocessing**

   1. **`sklearn.model_selection.train_test_split`**:
   A utility to split data into training and testing sets, ensuring the model is evaluated properly.
   2. **`sklearn.multioutput.MultiOutputClassifier`**:
   A method for fitting multiple classifiers for multi-output tasks, allowing prediction of multiple target variables.
   3. **`sklearn.feature_extraction.text.TfidfVectorizer`**:
   A method for converting text data into numerical features using term frequency-inverse document frequency (TF-IDF).
   4. **`sklearn.model_selection.GridSearchCV`**:
   Performs exhaustive search over specified hyperparameter values for a given model.
   5. **`sklearn.model_selection.RandomizedSearchCV`**:
   Randomized search over hyperparameter values to find optimal settings, often more efficient than grid search for large parameter spaces.
   6. **`sklearn.pipeline.Pipeline`**:
   Allows for chaining preprocessing steps and model fitting into a single workflow.
   7. **`transformers.BertTokenizer`**:  
   A tokenizer from the Transformers library that preprocesses raw text into token IDs for input to BERT models.
   8. **`transformers.BertModel`**:  
   A pre-trained BERT model from the Transformers library, often used for generating embeddings or fine-tuning for specific NLP tasks.

---

**Evaluation and Scoring Metrics**

   1. **`sklearn.metrics.accuracy_score`**:
    Measures the proportion of correct predictions.
   2. **`sklearn.metrics.classification_report`**:
   Provides a detailed report on classification metrics, such as precision, recall, and F1-score.
   3. **`sklearn.metrics.f1_score`**:
   Combines precision and recall into a single metric, suitable for imbalanced datasets.
   4. **`sklearn.metrics.precision_score`**:
   Measures the proportion of true positives out of all predicted positives.
   5. **`sklearn.metrics.recall_score`**:
    Measures the proportion of true positives out of all actual positives.
   6. **`sklearn.metrics.roc_curve` and `auc`**:
   Used to calculate and plot the Receiver Operating Characteristic (ROC) curve and the Area Under the Curve (AUC) score, indicating a model's ranking ability.
   7. **`sklearn.model_selection.learning_curve`**:  
   A tool for plotting learning curves to evaluate the training and validation performance.

---

**Additional Libraries**

   1. **`sklearn.exceptions.UndefinedMetricWarning`**:
   Handles warnings when a metric cannot be defined (e.g., no positive predictions).
   2. **`sklearn.preprocessing.LabelBinarizer`**:
   Converts categorical labels into a binary format, useful for multi-class classification.
   3. **`matplotlib.pyplot`**:
   Used for plotting data and visualizing results, such as ROC curves and feature importance.
   4. **`seaborn`**:
   A statistical data visualization library that builds on matplotlib, offering a high-level interface for drawing attractive graphs.
   5. **`numpy`**:
   A library for numerical computing, often used for handling arrays and matrix operations.
   6. **`warnings`**:
   Allows for filtering and managing warnings generated during the execution of the code.
---
