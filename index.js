import {APIGatewayProxyEvent, APIGatewayProxyResult} from 'aws-lambda';
import {DynamoDB} from 'aws-sdk';


const handler = async (event) => {
    try {
        if (!event.body) {
            return {
                statusCode: 400,
                body: JSON.stringify({message: 'Request body is required'})
            };
        }
        const request = JSON.parse(event.body);

        // Validate request
        if (!request.projectId || !Array.isArray(request.tickets)) {
            return {
                statusCode: 400,
                body: JSON.stringify({message: 'Invalid request format'})
            };
        }

        //process analytics
        const metrics = await processTickets(request);

        // Store in DynamoDB
        await storeMetrics(metrics);

        return {
            statusCode: 200,
            body: JSON.stringify(metrics)
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
        low: 10,
        medium: 20,
        high: 30,
        critical: 40
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
        low: 95,
        medium: 90,
        high: 85,
        critical: 80
    }
}

/**
 * @param {MetricsResponse} metrics
 * @returns {Promise<void>}
 */
async function storeMetrics(metrics) {
    const dynamoDB = new DynamoDB.DocumentClient();
    const params = {
        TableName: 'MetricsHistory',
        Item: {
            ...metrics,
            ttl: Math.floor(Date.now() / 1000) + (365 * 24 * 60 * 60) // 365 days TTL
        }
    };
    await dynamoDB.put(params).promise();
}






