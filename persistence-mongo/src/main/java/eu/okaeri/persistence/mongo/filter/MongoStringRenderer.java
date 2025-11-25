package eu.okaeri.persistence.mongo.filter;

import eu.okaeri.persistence.filter.renderer.JsonStringRenderer;

/**
 * JSON string renderer for MongoDB.
 * Extends JsonStringRenderer which properly escapes backslashes and quotes per RFC 8259.
 */
public class MongoStringRenderer extends JsonStringRenderer {
}
