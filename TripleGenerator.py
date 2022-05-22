#In reference to Lab 5
import sys
sys.path.append("../")
from rdflib import Graph
from rdflib import URIRef, BNode, Literal
from rdflib import Namespace
from rdflib.namespace import OWL, RDF, RDFS, FOAF, XSD
from rdflib.util import guess_format
import pandas as pd
from isub import isub
from lookup import DBpediaLookup
import csv
import owlrl
import numpy as np
import Levenshtein as lev 
import re
from isub import isub

# Look-ups
from lookup import DBpediaLookup, WikidataAPI, GoogleKGLookup
from endpoints import DBpediaEndpoint, WikidataEndpoint


class TripleGeneratingClass(object):
    
    def __init__(self, input_file):
        
        self.file = input_file
         #Load data in dataframe  
        self.data_frame = pd.read_csv(self.file, sep=',', quotechar='"',escapechar="\\").astype(str)
        for column in self.data_frame.columns.values:
            for idx in self.data_frame[self.data_frame[column] == 'nan'].index:
                self.data_frame.loc[idx, column] = ""

        
        #Dictionary that keeps the URIs. Specially useful if accessing a remote service to get a candidate URI to avoid repeated calls
        self.stringToURI = dict()
        #1. GRAPH INITIALIZATION
        #Empty graph
        self.g = Graph()
        self.g.parse("cw_onto.ttl")
    
        #Note that this is the same namespace used in the ontology "ontology_lab5.ttl"
        self.namespace_string= "http://www.semanticweb.org/in3067-inm713/restaurants#"
        
        #Special namspaces class to create directly URIRefs in python.           
        self.cw_onto = Namespace(self.namespace_string)

        #Prefixes for the serialization
        self.g.bind("cw", self.cw_onto)
                
        #KG
        self.dbpedia = DBpediaLookup()

    
    def Task1(self):
        self.ConvertCSVToRDF(False)
        
    def Task2(self):
        self.ConvertCSVToRDF(True)

    def ConvertCSVToRDF(self, useExternalURI):
        noExternalURI = False
        ### Type triple
        if 'currency' in self.data_frame:
            self.mappingToCreateTypeTriple('currency', self.cw_onto.Currency, noExternalURI)
        if 'item value' in self.data_frame:
            self.mappingToCreateTypeTriple('item value', self.cw_onto.ItemValue, noExternalURI)
            #self.mappingToCreateLiteralTriple('item value', 'item value', self.cw_onto.hasValue, XSD.double)
            self.mappingToCreateObjectTriple('item value', 'currency', self.cw_onto.amountCurrency)
        
            


        if 'name' in self.data_frame:
            self.mappingToCreateTypeTriple('name', self.cw_onto.Restaurant, noExternalURI)
            self.mappingToCreateLiteralTriple('name', 'name', self.cw_onto.restaurantName,XSD.string)
            self.mappingToCreateLiteralTriple('name', 'item value', self.cw_onto.amount, XSD.string)
            self.mappingToCreateObjectTriple('name', 'item value', self.cw_onto.hasValue)
            #self.mappingToCreateObjectTriple('menu item', 'name', self.cw_onto.servedInRestaurant)
            if 'menu item' in self.data_frame:
                
                self.mappingToCreateTypeTriple('menu item', self.cw_onto.MenuItem, noExternalURI)
               
                self.mappingToCreateLiteralTriple('name', 'menu item', self.cw_onto.itemName, XSD.string)
                self.mappingToCreateLiteralTriple('menu item', 'menu item', self.cw_onto.itemName, XSD.string)
                self.mappingToCreateObjectTriple('name','menu item', self.cw_onto.servesMenuItem)
                #self.mappingToCreateObjectTriple('menu item', 'name',self.cw_onto.servedInRestaurant)

                if 'item value' in self.data_frame:
                    self.mappingToCreateObjectTriple('menu item', 'item value', self.cw_onto.hasValue)
                    self.mappingToCreateLiteralTriple('menu item' ,'item value', self.cw_onto.amount, XSD.double)
                    
                if 'item description' in self.data_frame:
                    self.mappingToCreateLiteralTriple('menu item', 'item description', self.cw_onto.name, XSD.string)
                
                if 'name' in self.data_frame:
                    self.mappingToCreateLiteralTriple('menu item', 'name', self.cw_onto.servedInRestaurant, XSD.string)


            if 'address' in self.data_frame:
                self.mappingToCreateTypeTriple('address', self.cw_onto.Address, noExternalURI)
                self.mappingToCreateLiteralTriple('name', 'address', self.cw_onto.firstLineAddress, XSD.string)
                self.mappingToCreateObjectTriple('name', 'address', self.cw_onto.hasAddress)

            if 'postcode' in self.data_frame:
                self.mappingToCreateLiteralTriple('name', 'postcode', self.cw_onto.postCode, XSD.string)

            if 'city' in self.data_frame:
                self.mappingToCreateTypeTriple('city', self.cw_onto.City, useExternalURI)
                self.mappingToCreateLiteralTriple('name', 'city', self.cw_onto.City, XSD.string)
                self.mappingToCreateObjectTriple('name', 'city', self.cw_onto.hasCity)

            if 'country' in self.data_frame:
                self.mappingToCreateTypeTriple('country', self.cw_onto.Country, useExternalURI)
                self.mappingToCreateLiteralTriple('name','country', self.cw_onto.Country, XSD.string)
            
            if 'state' in self.data_frame:
                self.mappingToCreateTypeTriple('state', self.cw_onto.State, useExternalURI)
                self.mappingToCreateLiteralTriple('name','state', self.cw_onto.State, XSD.string)
                self.mappingToCreateObjectTriple('name','state', self.cw_onto.hasState)

            if 'categories' in self.data_frame:
                self.mappingToCreateTypeTriple('categories', self.cw_onto.Restaurant, noExternalURI)
                self.mappingToCreateLiteralTriple('name','categories', self.cw_onto.Restaurant, XSD.string)
                self.mappingToCreateObjectTriple('name','categories', self.cw_onto.servesMenuItem)
                
     
    def createURIForEntity(self, name, useExternalURI):
        #We create fresh URI (default option)
        self.stringToURI[name] = self.namespace_string + name.replace(" ", "_").replace(",", "_").replace("(", "_").replace(")", "_")
        
        if useExternalURI: #We connect to online KG
            uri = self.getExternalKGURI(name)
            if uri!="":
                self.stringToURI[name]=uri
        
        return self.stringToURI[name]

        
    def getExternalKGURI(self, name):
        entities = self.dbpedia.getKGEntities(name, 5)
        #print("Entities from DBPedia:")
        current_sim = -1
        current_uri=''
        for ent in entities:           
            isub_score = isub(name, ent.label) 
            if current_sim < isub_score:
                current_uri = ent.ident
                current_sim = isub_score
        
        return current_uri 
            
    def mappingToCreateTypeTriple(self, subject_column, class_type, useExternalURI):
        
        for subject in self.data_frame[subject_column]:
            subject = str(subject)
            if subject.lower() in self.stringToURI:
                
                entity_uri=self.stringToURI[subject.lower()]
            else:
                entity_uri=self.createURIForEntity(subject.lower(), useExternalURI)
            
            self.g.add((URIRef(entity_uri), RDF.type, class_type))
    def is_nan(self, x):
        return (x != x)
            
            
    #Mappings to create literal triples
    def mappingToCreateLiteralTriple(self, subject_column, object_column, predicate, datatype):
        
        for subject, lit_value in zip(self.data_frame[subject_column], self.data_frame[object_column]):
            
            if self.is_nan(lit_value) or lit_value==None or lit_value=="":
                continue
            
            else:
                #Uri as already created
                entity_uri=self.stringToURI[subject.lower()]
                    
                #Literal
                lit = Literal(lit_value, datatype=datatype)
                
                #New triple
                self.g.add((URIRef(entity_uri), predicate, lit))
     

    
    def mappingToCreateObjectTriple(self, subject_column, object_column, predicate):
        
        for subject, object in zip(self.data_frame[subject_column], self.data_frame[object_column]):

            if self.is_nan(subject) or self.is_nan(object):
                pass
            
            else:
                object = str(object)
                #Uri as already created
                subject_uri=self.stringToURI[subject.lower()]
                object_uri=self.stringToURI[object.lower()]
                #New triple
                self.g.add((URIRef(subject_uri), predicate, URIRef(object_uri)))
    
    def performReasoning(self, ontology_file):    

        print("Data triples from CSV: '" + str(len(self.g)) + "'.")
    

        self.g.load(ontology_file,  format=guess_format(ontology_file)) #e.g., format=ttl
        
        print("Triples including ontology: '" + str(len(self.g)) + "'.")
        
        #Applying reasoning and expand the graph with new triples 
        owlrl.DeductiveClosure(owlrl.OWLRL_Semantics, axiomatic_triples=False, datatype_axioms=False).expand(self.g)
        
        print("Triples after OWL 2 RL reasoning: '" + str(len(self.g)) + "'.")
    
    def saveGraph(self, file_output):
        
        ##SAVE/SERIALIZE GRAPH
        #print(self.g.serialize(format="turtle").decode("utf-8"))
        self.g.serialize(destination=file_output, format='ttl')