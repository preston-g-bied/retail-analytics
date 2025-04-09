// config/database/init-scripts/mongodb/01_init_mongodb.js
// MongoDB initialization script

db = db.getSiblingDB('retail_analytics')

// create collections with validation
db.createCollection('products', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['product_key', 'name', 'created_at'],
            properties: {
                product_key: {
                    bsonType: 'string',
                    description: 'Natural key from source system'
                },
                name: {
                    bsonType: 'string',
                    description: 'Product name'
                },
                description: {
                    bsonType: 'string',
                    description: 'Product description'
                },
                category: {
                    bsonType: 'string',
                    description: 'Product category'
                },
                subcategory: {
                    bsonType: 'string',
                    description: 'Product subcategory'
                },
                brand: {
                    bsonType: 'string',
                    description: 'Product brand'
                },
                attributes: {
                    bsonType: 'object',
                    description: 'Product attributes in key-value pairs'
                },
                images: {
                    bsonType: 'array',
                    description: 'Array of image URLs',
                    items: {
                        bsonType: 'string'
                    }
                },
                price: {
                    bsonType: 'object',
                    required: ['current'],
                    properties: {
                        current: {
                            bsonType: 'double',
                            description: 'Current price'
                        },
                        currency: {
                            bsonType: 'string',
                            description: 'Currency code (e.g., USD, EUR)'
                        },
                        history: {
                            bsonType: 'array',
                            description: 'Price history',
                            items: {
                                bsonType: 'object',
                                required: ['price', 'effective_date'],
                                properties: {
                                    price: {
                                        bsonType: 'double'
                                    },
                                    effective_date: {
                                        bsonType: 'date'
                                    }
                                }
                            }
                        }
                    }
                },
                inventory: {
                    bsonType: 'object',
                    properties: {
                        quantity: {
                            bsonType: 'int',
                            description: 'Current inventory level'
                        },
                        warehouse_location: {
                            bsonType: 'string',
                            description: 'Warehouse identifier'
                        }
                    }
                },
                reviews: {
                    bsonType: 'array',
                    description: 'Product reviews',
                    items: {
                        bsonType: 'object',
                        required: ['user_id', 'rating', 'review_date'],
                        properties: {
                            user_id: {
                                bsonType: 'string'
                            },
                            rating: {
                                bsonType: 'int',
                                minimum: 1,
                                maximum: 5
                            },
                            review_text: {
                                bsonType: 'string'
                            },
                            review_date: {
                                bsonType: 'date'
                            },
                            helpful_votes: {
                                bsonType: 'int'
                            }
                        }
                    }
                },
                created_at: {
                    bsonType: 'date',
                    description: 'Creation timestamp'
                },
                updated_at: {
                    bsonType: 'date',
                    description: 'Last update timestamp'
                },
                source: {
                    bsonType: 'string',
                    description: 'Source of the data'
                }
            }
        }
    }
});

db.createCollection('customers', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['customer_key', 'created_at'],
            properties: {
                customer_key: {
                    bsonType: 'string',
                    description: 'Natural key from source system'
                },
                first_name: {
                    bsonType: 'string'
                },
                last_name: {
                    bsonType: 'string'
                },
                email: {
                    bsonType: 'string'
                },
                contact: {
                    bsonType: 'object',
                    properties: {
                        phone: {
                            bsonType: 'string'
                        },
                        alternative_email: {
                            bsonType: 'string'
                        }
                    }
                },
                addresses: {
                    bsonType: 'array',
                    items: {
                        bsonType: 'object',
                        required: ['type', 'address_line1', 'city', 'country'],
                        properties: {
                            type: {
                                bsonType: 'string',
                                enum: ['billing', 'shipping', 'both']
                            },
                            is_default: {
                                bsonType: 'bool'
                            },
                            address_line1: {
                                bsonType: 'string'
                            },
                            address_line2: {
                                bsonType: 'string'
                            },
                            city: {
                                bsonType: 'string'
                            },
                            state: {
                                bsonType: 'string'
                            },
                            postal_code: {
                                bsonType: 'string'
                            },
                            country: {
                                bsonType: 'string'
                            }
                        }
                    }
                },
                preferences: {
                    bsonType: 'object',
                    properties: {
                        communication_preferences: {
                            bsonType: 'object',
                            properties: {
                                email_subscribed: {
                                    bsonType: 'bool'
                                },
                                sms_subscribed: {
                                    bsonType: 'bool'
                                }
                            }
                        },
                        favorite_categories: {
                            bsonType: 'array',
                            items: {
                                bsonType: 'string'
                            }
                        }
                    }
                },
                demographics: {
                    bsonType: 'object',
                    properties: {
                        birth_date: {
                            bsonType: 'date'
                        },
                        gender: {
                            bsonType: 'string'
                        },
                        income_bracket: {
                            bsonType: 'string'
                        }
                    }
                },
                profile: {
                    bsonType: 'object',
                    properties: {
                        profile_picture: {
                            bsonType: 'string'
                        },
                        bio: {
                            bsonType: 'string'
                        }
                    }
                },
                created_at: {
                    bsonType: 'date'
                },
                updated_at: {
                    bsonType: 'date'
                },
                source: {
                    bsonType: 'string'
                },
                is_active: {
                    bsonType: 'bool'
                }
            }
        }
    }
});

db.createCollection('browsing_history', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['customer_key', 'product_key', 'timestamp'],
            properties: {
                customer_key: {
                    bsonType: 'string'
                },
                product_key: {
                    bsonType: 'string'
                },
                timestamp: {
                    bsonType: 'date'
                },
                session_id: {
                    bsonType: 'string'
                },
                device: {
                    bsonType: 'string'
                },
                os: {
                    bsonType: 'string'
                },
                browser: {
                    bsonType: 'string'
                },
                ip_address: {
                    bsonType: 'string'
                },
                referer: {
                    bsonType: 'string'
                },
                time_spent_seconds: {
                    bsonType: 'int'
                },
                actions: {
                    bsonType: 'array',
                    items: {
                        bsonType: 'object',
                        properties: {
                            action_type: {
                                bsonType: 'string',
                                enum: ['view', 'add_to_cart', 'add_to_wishlist', 'remove_from_cart', 'start_checkout']
                            },
                            timestamp: {
                                bsonType: 'date'
                            }
                        }
                    }
                }
            }
        }
    }
});

db.createCollection('carts', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['cart_id', 'customer_key', 'created_at', 'status'],
            properties: {
                cart_id: {
                    bsonType: 'string'
                },
                customer_key: {
                    bsonType: 'string'
                },
                session_id: {
                    bsonType: 'string'
                },
                status: {
                    bsonType: 'string',
                    enum: ['active', 'abandoned', 'converted', 'merged']
                },
                items: {
                    bsonType: 'array',
                    items: {
                        bsonType: 'object',
                        required: ['product_key', 'quantity', 'added_at'],
                        properties: {
                            product_key: {
                                bsonType: 'string'
                            },
                            quantity: {
                                bsonType: 'int'
                            },
                            price_at_add: {
                                bsonType: 'double'
                            },
                            added_at: {
                                bsonType: 'date'
                            }
                        }
                    }
                },
                created_at: {
                    bsonType: 'date'
                },
                last_activity: {
                    bsonType: 'date'
                },
                conversion_info: {
                    bsonType: 'object',
                    properties: {
                        converted_at: {
                            bsonType: 'date'
                        },
                        transaction_key: {
                            bsonType: 'string'
                        }
                    }
                }
            }
        }
    }
});

// create indexes
db.products.createIndex({ "product_key": 1 }, { unique: true });
db.products.createIndex({ "category": 1, "subcategory": 1 });
db.products.createIndex({ "name": "text", "description": "text" });

db.customers.createIndex({ "customer_key": 1 }, { unique: true });
db.customers.createIndex({ "email": 1 }, { unique: true, sparse: true });

db.browsing_history.createIndex({ "customer_key": 1, "timestamp": -1 });
db.browsing_history.createIndex({ "product_key": 1, "timestamp": -1 });
db.browsing_history.createIndex({ "session_id": 1 });

db.carts.createIndex({ "cart_id": 1 }, { unique: true });
db.carts.createIndex({ "customer_key": 1 });
db.carts.createIndex({ "status": 1, "last_activity": 1 });

// create users and set permissions
db.createUser({
    user: "etl_user",
    pwd: "etl_password",
    roles: [
        { role: "readWrite", db: "retail_analytics" }
    ]
});

db.createUser({
    user: "analytics_user",
    pwd: "analytics_password",
    roles: [
        { role: "read", db: "retail_analytics" }
    ]
});

db.createUser({
    user: "app_user",
    pwd: "app_password",
    roles: [
        { role: "read", db: "retail_analytics" }
    ]
});