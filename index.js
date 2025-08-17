// index.js - Optimized Jira Analytics Lambda function
const { DynamoDB } = require('aws-sdk');

// Initialize DynamoDB client outside handler for connection reuse
const dynamoDB = new DynamoDB.DocumentClient({
    region: process.env.AWS_REGION || 'us-east-1',
    maxRetries: 3,
    retryDelayOptions: {
        customBackoff: function(retryCount) {
            return Math.pow(2, retryCount) * 100; // Exponential backoff
        }
    }
});

/**
 * Main Lambda handler for Jira ticket analytics
 * @param {Object} event - Lambda event object
 * @returns {Object} HTTP response object
 */
const handler = async (event) => {
    const startTime = Date.now();
    console.log('Event received:', JSON.stringify(event, null, 2));

    try {
        // Validate request structure
        const validationResult = validateRequest(event);
        if (!validationResult.isValid) {
            console.error('Validation failed:', validationResult.errors);
            return {
                statusCode: 400,
                headers: {
                    'Content-Type': 'application/json',
                    'X-Request-ID': generateRequestId()
                },
                body: JSON.stringify({
                    message: 'Invalid request format',
                    errors: validationResult.errors
                })
            };
        }

        // Process analytics with performance monitoring
        console.log(`Processing ${event.tickets.length} tickets for project ${event.projectId}`);
        const metrics = await processTickets(event);
        console.log('Processed metrics:', JSON.stringify(metrics, null, 2));

        // Store in DynamoDB with retry logic
        const storedMetrics = await storeMetrics(metrics);
        console.log('Successfully stored metrics in DynamoDB');

        const duration = Date.now() - startTime;
        console.log(`Request completed in ${duration}ms`);

        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json',
                'X-Request-ID': generateRequestId(),
                'X-Processing-Time': `${duration}ms`
            },
            body: JSON.stringify({
                message: 'Analytics processed successfully',
                data: storedMetrics,
                meta: {
                    processingTimeMs: duration,
                    ticketsProcessed: event.tickets.length
                }
            })
        };

    } catch (error) {
        const duration = Date.now() - startTime;
        console.error('Error processing request:', {
            error: error.message,
            stack: error.stack,
            duration: `${duration}ms`,
            event: JSON.stringify(event, null, 2)
        });

        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json',
                'X-Request-ID': generateRequestId()
            },
            body: JSON.stringify({
                message: 'Internal Server Error',
                error: process.env.NODE_ENV === 'development' ? error.message : 'An unexpected error occurred'
            })
        };
    }
};

/**
 * Validate incoming request structure
 * @param {Object} event - Lambda event object
 * @returns {Object} Validation result
 */
function validateRequest(event) {
    const errors = [];

    if (!event.projectId || typeof event.projectId !== 'string') {
        errors.push('projectId is required and must be a string');
    }

    if (!Array.isArray(event.tickets)) {
        errors.push('tickets must be an array');
    } else if (event.tickets.length === 0) {
        errors.push('tickets array cannot be empty');
    } else {
        // Validate individual tickets
        event.tickets.forEach((ticket, index) => {
            if (!ticket.id) {
                errors.push(`Ticket ${index}: id is required`);
            }
            if (!ticket.severity) {
                errors.push(`Ticket ${index}: severity is required`);
            }
            if (!['low', 'medium', 'high', 'critical'].includes(ticket.severity?.toLowerCase())) {
                errors.push(`Ticket ${index}: severity must be one of: low, medium, high, critical`);
            }
        });
    }

    return {
        isValid: errors.length === 0,
        errors
    };
}

/**
 * Process tickets and calculate analytics metrics
 * @param {Object} request - Validated request object
 * @returns {Object} Calculated metrics
 */
async function processTickets(request) {
    const metrics = {
        projectId: request.projectId,
        timestamp: new Date().toISOString(),
        severityDistribution: calculateSeverityDistribution(request.tickets),
        averageResolutionTimes: calculateResolutionTimes(request.tickets),
        slaCompliance: calculateSLACompliance(request.tickets),
        ticketCount: request.tickets.length,
        openTickets: request.tickets.filter(t => t.status !== 'resolved' && t.status !== 'closed').length,
        resolvedTickets: request.tickets.filter(t => t.status === 'resolved' || t.status === 'closed').length
    };

    return metrics;
}

/**
 * Calculate severity distribution as percentages
 * @param {Array} tickets - Array of ticket objects
 * @returns {Object} Severity distribution percentages
 */
function calculateSeverityDistribution(tickets) {
    const distribution = {
        low: 0,
        medium: 0,
        high: 0,
        critical: 0
    };

    const total = tickets.length;

    tickets.forEach(ticket => {
        const severity = ticket.severity?.toLowerCase();
        if (distribution[severity] !== undefined) {
            distribution[severity]++;
        }
    });

    // Convert to percentages with one decimal place
    Object.keys(distribution).forEach(key => {
        distribution[key] = total > 0 ? parseFloat(((distribution[key] / total) * 100).toFixed(1)) : 0;
    });

    return distribution;
}

/**
 * Calculate average resolution times per severity level
 * @param {Array} tickets - Array of ticket objects
 * @returns {Object} Average resolution times in hours
 */
function calculateResolutionTimes(tickets) {
    const resolutionTimes = {
        low: 0,
        medium: 0,
        high: 0,
        critical: 0
    };

    const counts = {
        low: 0,
        medium: 0,
        high: 0,
        critical: 0
    };

    tickets.forEach(ticket => {
        if (ticket.createdDate && ticket.resolvedDate) {
            const severity = ticket.severity?.toLowerCase();
            if (resolutionTimes[severity] !== undefined) {
                const created = new Date(ticket.createdDate);
                const resolved = new Date(ticket.resolvedDate);
                const hoursToResolve = (resolved - created) / (1000 * 60 * 60); // Convert to hours

                resolutionTimes[severity] += hoursToResolve;
                counts[severity]++;
            }
        }
    });

    // Calculate averages
    Object.keys(resolutionTimes).forEach(severity => {
        if (counts[severity] > 0) {
            resolutionTimes[severity] = parseFloat((resolutionTimes[severity] / counts[severity]).toFixed(1));
        }
    });

    return resolutionTimes;
}

/**
 * Calculate SLA compliance rates per severity level
 * @param {Array} tickets - Array of ticket objects
 * @returns {Object} SLA compliance percentages
 */
function calculateSLACompliance(tickets) {
    // SLA targets in hours
    const slaTargets = {
        critical: 4,   // 4 hours
        high: 24,      // 1 day
        medium: 72,    // 3 days
        low: 168       // 1 week
    };

    const compliance = {
        low: 0,
        medium: 0,
        high: 0,
        critical: 0
    };

    const counts = {
        low: 0,
        medium: 0,
        high: 0,
        critical: 0
    };

    tickets.forEach(ticket => {
        if (ticket.createdDate && ticket.resolvedDate) {
            const severity = ticket.severity?.toLowerCase();
            if (slaTargets[severity] !== undefined) {
                const created = new Date(ticket.createdDate);
                const resolved = new Date(ticket.resolvedDate);
                const hoursToResolve = (resolved - created) / (1000 * 60 * 60);

                if (hoursToResolve <= slaTargets[severity]) {
                    compliance[severity]++;
                }
                counts[severity]++;
            }
        }
    });

    // Calculate compliance percentages
    Object.keys(compliance).forEach(severity => {
        if (counts[severity] > 0) {
            compliance[severity] = parseFloat(((compliance[severity] / counts[severity]) * 100).toFixed(1));
        }
    });

    return compliance;
}

/**
 * Store metrics in DynamoDB with enhanced error handling
 * @param {Object} metrics - Calculated metrics object
 * @returns {Object} Stored metrics from DynamoDB
 */
async function storeMetrics(metrics) {
    console.log('Storing metrics in DynamoDB...');

    const params = {
        TableName: process.env.DYNAMODB_TABLE || 'MetricsHistory',
        Key: {
            projectId: metrics.projectId,
            timestamp: metrics.timestamp
        },
        UpdateExpression: `SET 
            severityDistribution = :sd,
            averageResolutionTimes = :art,
            slaCompliance = :slac,
            ticketCount = :tc,
            openTickets = :ot,
            resolvedTickets = :rt,
            #ttlField = if_not_exists(#ttlField, :ttl),
            updatedAt = :updatedAt
        `,
        ExpressionAttributeNames: {
            '#ttlField': 'ttl'  // Handle reserved keyword
        },
        ExpressionAttributeValues: {
            ':sd': metrics.severityDistribution,
            ':art': metrics.averageResolutionTimes,
            ':slac': metrics.slaCompliance,
            ':tc': metrics.ticketCount,
            ':ot': metrics.openTickets,
            ':rt': metrics.resolvedTickets,
            ':ttl': Math.floor(Date.now() / 1000) + (365 * 24 * 60 * 60), // 1 year TTL
            ':updatedAt': new Date().toISOString()
        },
        ReturnValues: 'ALL_NEW'
    };

    try {
        const result = await dynamoDB.update(params).promise();
        console.log('Successfully stored metrics in DynamoDB');
        return result.Attributes;

    } catch (error) {
        console.error('DynamoDB operation failed:', {
            error: error.message,
            code: error.code,
            requestId: error.requestId,
            statusCode: error.statusCode
        });

        // Provide more specific error messages
        if (error.code === 'ResourceNotFoundException') {
            throw new Error(`DynamoDB table '${params.TableName}' not found`);
        } else if (error.code === 'ProvisionedThroughputExceededException') {
            throw new Error('DynamoDB write capacity exceeded. Please try again later.');
        } else if (error.code === 'ValidationException') {
            throw new Error(`DynamoDB validation error: ${error.message}`);
        }

        throw error;
    }
}

/**
 * Generate a unique request ID for tracking
 * @returns {string} Request ID
 */
function generateRequestId() {
    return `req-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

module.exports = { handler };