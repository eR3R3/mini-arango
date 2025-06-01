use std::f64::consts::PI;
use crate::common::error::{ArangoError, Result};
use super::indexes::{IndexDefinition, IndexStatistics, FilterCosts};

/// Geographic point representation
#[derive(Debug, Clone, PartialEq)]
pub struct GeoPoint {
    pub latitude: f64,
    pub longitude: f64,
}

impl GeoPoint {
    pub fn new(lat: f64, lng: f64) -> Result<Self> {
        if lat < -90.0 || lat > 90.0 {
            return Err(ArangoError::bad_parameter(format!("Invalid latitude: {}", lat)));
        }
        if lng < -180.0 || lng > 180.0 {
            return Err(ArangoError::bad_parameter(format!("Invalid longitude: {}", lng)));
        }
        
        Ok(GeoPoint {
            latitude: lat,
            longitude: lng,
        })
    }
    
    /// Calculate distance between two points using Haversine formula
    pub fn distance_to(&self, other: &GeoPoint) -> f64 {
        let earth_radius = 6371000.0; // Earth radius in meters
        
        let lat1_rad = self.latitude.to_radians();
        let lat2_rad = other.latitude.to_radians();
        let delta_lat = (other.latitude - self.latitude).to_radians();
        let delta_lng = (other.longitude - self.longitude).to_radians();
        
        let a = (delta_lat / 2.0).sin().powi(2) +
                lat1_rad.cos() * lat2_rad.cos() * (delta_lng / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        
        earth_radius * c
    }
    
    /// Check if point is within a bounding box
    pub fn within_bbox(&self, min_lat: f64, min_lng: f64, max_lat: f64, max_lng: f64) -> bool {
        self.latitude >= min_lat && self.latitude <= max_lat &&
        self.longitude >= min_lng && self.longitude <= max_lng
    }
    
    /// Check if point is within a circle
    pub fn within_circle(&self, center: &GeoPoint, radius: f64) -> bool {
        self.distance_to(center) <= radius
    }
}

/// Bounding box for geographic queries
#[derive(Debug, Clone)]
pub struct BoundingBox {
    pub min_lat: f64,
    pub min_lng: f64,
    pub max_lat: f64,
    pub max_lng: f64,
}

impl BoundingBox {
    pub fn new(min_lat: f64, min_lng: f64, max_lat: f64, max_lng: f64) -> Result<Self> {
        if min_lat > max_lat || min_lng > max_lng {
            return Err(ArangoError::bad_parameter("Invalid bounding box"));
        }
        
        Ok(BoundingBox {
            min_lat,
            min_lng,
            max_lat,
            max_lng,
        })
    }
    
    pub fn contains(&self, point: &GeoPoint) -> bool {
        point.within_bbox(self.min_lat, self.min_lng, self.max_lat, self.max_lng)
    }
    
    pub fn intersects(&self, other: &BoundingBox) -> bool {
        !(other.max_lat < self.min_lat || other.min_lat > self.max_lat ||
          other.max_lng < self.min_lng || other.min_lng > self.max_lng)
    }
}

/// Geo index node for spatial indexing (simplified R-tree)
#[derive(Debug, Clone)]
pub struct GeoIndexNode {
    pub bbox: BoundingBox,
    pub points: Vec<(GeoPoint, Vec<u8>)>, // (point, document_id)
    pub children: Vec<GeoIndexNode>,
    pub is_leaf: bool,
}

impl GeoIndexNode {
    pub fn new_leaf() -> Self {
        GeoIndexNode {
            bbox: BoundingBox::new(-90.0, -180.0, 90.0, 180.0).unwrap(),
            points: Vec::new(),
            children: Vec::new(),
            is_leaf: true,
        }
    }
    
    pub fn insert(&mut self, point: GeoPoint, doc_id: Vec<u8>) {
        if self.is_leaf {
            self.points.push((point, doc_id));
            self.update_bbox();
            
            // Split if too many points (simple threshold)
            if self.points.len() > 10 {
                self.split();
            }
        } else {
            // Find best child node to insert into
            let mut best_child = 0;
            let mut min_expansion = f64::INFINITY;
            
            for (i, child) in self.children.iter().enumerate() {
                let expansion = self.calculate_bbox_expansion(&child.bbox, &point);
                if expansion < min_expansion {
                    min_expansion = expansion;
                    best_child = i;
                }
            }
            
            self.children[best_child].insert(point, doc_id);
            self.update_bbox();
        }
    }
    
    pub fn search_within_radius(&self, center: &GeoPoint, radius: f64) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        
        // Check if this node's bbox intersects with the search circle
        if !self.bbox_intersects_circle(center, radius) {
            return result;
        }
        
        if self.is_leaf {
            for (point, doc_id) in &self.points {
                if point.within_circle(center, radius) {
                    result.push(doc_id.clone());
                }
            }
        } else {
            for child in &self.children {
                result.extend(child.search_within_radius(center, radius));
            }
        }
        
        result
    }
    
    pub fn search_within_bbox(&self, bbox: &BoundingBox) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        
        // Check if this node's bbox intersects with the search bbox
        if !self.bbox.intersects(bbox) {
            return result;
        }
        
        if self.is_leaf {
            for (point, doc_id) in &self.points {
                if bbox.contains(point) {
                    result.push(doc_id.clone());
                }
            }
        } else {
            for child in &self.children {
                result.extend(child.search_within_bbox(bbox));
            }
        }
        
        result
    }
    
    fn split(&mut self) {
        // Simple split: divide points into two groups
        if self.points.len() <= 1 {
            return;
        }
        
        self.is_leaf = false;
        let mid = self.points.len() / 2;
        
        // Sort by longitude for simple split
        self.points.sort_by(|a, b| a.0.longitude.partial_cmp(&b.0.longitude).unwrap());
        
        let mut left_child = GeoIndexNode::new_leaf();
        let mut right_child = GeoIndexNode::new_leaf();
        
        for (point, doc_id) in self.points.drain(..mid) {
            left_child.points.push((point, doc_id));
        }
        for (point, doc_id) in self.points.drain(..) {
            right_child.points.push((point, doc_id));
        }
        
        left_child.update_bbox();
        right_child.update_bbox();
        
        self.children.push(left_child);
        self.children.push(right_child);
    }
    
    fn update_bbox(&mut self) {
        if self.is_leaf && !self.points.is_empty() {
            let mut min_lat = f64::INFINITY;
            let mut max_lat = f64::NEG_INFINITY;
            let mut min_lng = f64::INFINITY;
            let mut max_lng = f64::NEG_INFINITY;
            
            for (point, _) in &self.points {
                min_lat = min_lat.min(point.latitude);
                max_lat = max_lat.max(point.latitude);
                min_lng = min_lng.min(point.longitude);
                max_lng = max_lng.max(point.longitude);
            }
            
            self.bbox = BoundingBox::new(min_lat, min_lng, max_lat, max_lng).unwrap();
        } else if !self.children.is_empty() {
            let mut min_lat = f64::INFINITY;
            let mut max_lat = f64::NEG_INFINITY;
            let mut min_lng = f64::INFINITY;
            let mut max_lng = f64::NEG_INFINITY;
            
            for child in &self.children {
                min_lat = min_lat.min(child.bbox.min_lat);
                max_lat = max_lat.max(child.bbox.max_lat);
                min_lng = min_lng.min(child.bbox.min_lng);
                max_lng = max_lng.max(child.bbox.max_lng);
            }
            
            self.bbox = BoundingBox::new(min_lat, min_lng, max_lat, max_lng).unwrap();
        }
    }
    
    fn calculate_bbox_expansion(&self, bbox: &BoundingBox, point: &GeoPoint) -> f64 {
        let new_min_lat = bbox.min_lat.min(point.latitude);
        let new_max_lat = bbox.max_lat.max(point.latitude);
        let new_min_lng = bbox.min_lng.min(point.longitude);
        let new_max_lng = bbox.max_lng.max(point.longitude);
        
        let original_area = (bbox.max_lat - bbox.min_lat) * (bbox.max_lng - bbox.min_lng);
        let new_area = (new_max_lat - new_min_lat) * (new_max_lng - new_min_lng);
        
        new_area - original_area
    }
    
    fn bbox_intersects_circle(&self, center: &GeoPoint, radius: f64) -> bool {
        // Simplified check: test if circle intersects with bounding box
        let closest_lat = center.latitude.max(self.bbox.min_lat).min(self.bbox.max_lat);
        let closest_lng = center.longitude.max(self.bbox.min_lng).min(self.bbox.max_lng);
        
        let closest_point = GeoPoint::new(closest_lat, closest_lng).unwrap();
        center.distance_to(&closest_point) <= radius
    }
}

/// Geo index implementation
pub struct GeoIndex {
    definition: IndexDefinition,
    statistics: IndexStatistics,
    root: GeoIndexNode,
}

impl GeoIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        let statistics = IndexStatistics {
            name: definition.name.clone(),
            size: 0,
            memory_usage: 0,
            selectivity: 1.0,
            usage_count: 0,
            last_used: None,
            estimated_items: 0,
            estimated_cost: 0.0,
            cache_hit_rate: Some(0.0),
            index_efficiency: 1.0,
        };
        
        GeoIndex {
            definition,
            statistics,
            root: GeoIndexNode::new_leaf(),
        }
    }
    
    pub fn insert(&mut self, point: GeoPoint, doc_id: Vec<u8>) -> Result<()> {
        self.root.insert(point, doc_id);
        self.statistics.size += 1;
        self.statistics.estimated_items += 1;
        Ok(())
    }
    
    pub fn search_near(&self, center: GeoPoint, radius: f64) -> Vec<Vec<u8>> {
        self.root.search_within_radius(&center, radius)
    }
    
    pub fn search_within(&self, bbox: BoundingBox) -> Vec<Vec<u8>> {
        self.root.search_within_bbox(&bbox)
    }
    
    pub fn calculate_filter_costs(&self, query_type: &str) -> FilterCosts {
        let base_cost = match query_type {
            "near" => (self.statistics.estimated_items as f64).sqrt(), // spatial index advantage
            "within" => (self.statistics.estimated_items as f64).log2(), // bbox queries are efficient
            _ => self.statistics.estimated_items as f64, // full scan
        };
        
        FilterCosts {
            supports_condition: true,
            covered_attributes: self.definition.fields.len(),
            estimated_items: self.statistics.estimated_items / 10, // assume 10% selectivity for geo queries
            estimated_costs: base_cost,
            selectivity_factor: 0.1,
        }
    }
    
    pub fn definition(&self) -> &IndexDefinition {
        &self.definition
    }
    
    pub fn statistics(&self) -> &IndexStatistics {
        &self.statistics
    }
} 