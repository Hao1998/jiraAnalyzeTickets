const {DynamoDB} = require('aws-sdk');


const handler = async (event) => {
    console.log('Event received:', JSON.stringify(event, null, 2));
    try {

        // Validate request
        if (!event.projectId || !Array.isArray(event.tickets)) {
            return {
                statusCode: 400,
                body: JSON.stringify({message: 'Invalid request format'})
            };
        }

        //process analytics
        const metrics = await processTickets(event);
        console.log('Processed metrics:', JSON.stringify(metrics));

        // Store in DynamoDB and get the result
        const storedMetrics = await storeMetrics(metrics);
        console.log("Stored metrics:", JSON.stringify(storedMetrics))

        return {
            statusCode: 200,
            body: JSON.stringify(storedMetrics)
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({message: 'Internal Server Error'})
        };
    }
}

async function processTickets(request) {
    const metrics = {
        projectId: request.projectId,
        timestamp: new Date().toISOString(),
        severityDistribution: calculateSeverityDistribution(request.tickets),
        averageResolutionTimes: calculateResolutionTimes(request.tickets),
        slaCompliance: calculateSLACompliance(request.tickets)
    };

    return metrics;
}


function calculateSeverityDistribution(tickets) {
    const distribution = {
        low: 0,
        medium: 0,
        high: 0,
        critical: 0
    };

    const total = tickets.length;
    tickets.forEach(ticket => {
        const severity = ticket.severity.toLowerCase();
        if (distribution[severity] !== undefined) {
            distribution[severity]++;
        }
    });

    //convert to percentages
    Object.keys(distribution).forEach(key => {
        distribution[key] = ((distribution[key] / total) * 100).toFixed(1);
    })

    return distribution;
}

/**
 * @param {Ticket[]} tickets
 * @returns {Object.<string, number>}
 */
function calculateResolutionTimes(tickets) {
    // Implementation for calculating average resolution times per severity
    // This is a placeholder - implement actual business logic
    return {
        low: 15,
        medium: 21,
        high: 34,
        critical: 47
    }
}

/**
 * @param {Ticket[]} tickets
 * @returns {Object.<string, number>}
 */
function calculateSLACompliance(tickets) {
    // Implementation for calculating SLA compliance rates per severity
    // This is a placeholder - implement actual business logic
    return {
        low: 42,
        medium: 88,
        high: 10,
        critical: 80
    }
}

/**
 * @param {{severityDistribution: {high: number, critical: number, low: number, medium: number}, slaCompliance: Object<string, number>, projectId, averageResolutionTimes: Object<string, number>, timestamp: string}} metrics
 * @returns {Promise<void>}
 */
async function storeMetrics(metrics) {
    const dynamoDB = new DynamoDB.DocumentClient();
    const params = {
        TableName: 'MetricsHistory',
        Key: {
            projectId: metrics.projectId,
            timestamp: metrics.timestamp
        },
        UpdateExpression: `SET 
                severityDistribution = :sd,
                averageResolutionTimes = :art,
                slaCompliance = :slac,
                #ttlField = if_not_exists(#ttlField, :ttl)
                `,
        ExpressionAttributeNames: {
            '#ttlField': 'ttl'  // Use ExpressionAttributeName for reserved keyword
        },
        ExpressionAttributeValues: {
            ':sd': metrics.severityDistribution,
            ':art': metrics.averageResolutionTimes,
            ':slac': metrics.slaCompliance,
            ':ttl': Math.floor(Date.now() / 1000) + (365 * 24 * 60 * 60)
        },
        ReturnValues: 'ALL_NEW' // This will return the item after the update
    };

    try {
        const result = await dynamoDB.update(params).promise();
        return result.Attributes; // Return the updated/created item
    } catch (error) {
        console.error('DynamoDB operation failed: ', error);
        throw error;
    }
}

module.exports = {handler};






