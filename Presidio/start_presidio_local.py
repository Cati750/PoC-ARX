# -*- coding: utf-8 -*-
"""
Created on Thu Jun  5 17:47:58 2025

@author: catim
"""

from presidio_analyzer import AnalyzerEngine, RecognizerRegistry, PatternRecognizer, Pattern
from flask import Flask, request, jsonify

# necessário correr este script para o presidio funcionar locamente com API REST
race_recognizer = PatternRecognizer(supported_entity="RACE",
                                      deny_list=["Caucasian","African American","Asian"])

bloodtype_recognizer = PatternRecognizer(supported_entity="BLOOD_TYPE",
                                      deny_list=["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"])

gender_recognizer = PatternRecognizer(supported_entity="GENDER",
                                      deny_list=["Female","Male","Other"])

pattern = Pattern(name="hospital_pattern", regex=r"\b[\w\s]*Hospital[\w\s]*\b", score=0.85)
#pattern exige um score (nível de confiança) uma vez que nao identifica string literais como o Pattern Recognizer (o score é sempre 1)
#Se "Starlight Hospital" corresponder a dois padrões: Um padrão reconhece com score=0.85 como FACILITY; Outro reconhece com score=0.60 como ORGANIZATION
# o presidio vai dar mais peso à detecção como FACILITY.

facility_recognizer = PatternRecognizer(supported_entity="FACILITY",
                                      patterns=[pattern])

registry = RecognizerRegistry()
registry.load_predefined_recognizers()

# Add the recognizer to the existing list of recognizers
registry.add_recognizer(race_recognizer)
registry.add_recognizer(bloodtype_recognizer)
registry.add_recognizer(gender_recognizer)
registry.add_recognizer(facility_recognizer)

# Set up analyzer with our updated recognizer registry
analyzer = AnalyzerEngine(registry=registry)

app = Flask(__name__)

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.get_json()
    text = data.get("text", "")
    language = data.get("language", "en")
    results = analyzer.analyze(text=text, language=language)
    return jsonify([r.to_dict() for r in results])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
