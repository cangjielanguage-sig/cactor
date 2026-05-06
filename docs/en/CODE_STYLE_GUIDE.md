# CActor Code Style Guide

## Overview

This guide defines the code style standards for the CActor project, ensuring consistency, readability, and maintainability.

## Naming Conventions

### Class Names (PascalCase)
```cangjie
// Correct
class ActorSystem
class MessageProcessor
class ErrorHandlingManager

// Incorrect
class actorSystem
class message_processor
class errorhandlingmanager
```

### Interface Names (PascalCase)
```cangjie
// Correct
interface Actor
interface MessageHandler
interface ErrorHandlingStrategy

// Incorrect
interface actor
interface messageHandler
interface error_handling_strategy
```

### Function Names (camelCase)
```cangjie
// Correct
func processMessage()
func handleError()
func createActorSystem()

// Incorrect
func ProcessMessage()
func handle_error()
func CreateActorSystem()
```

### Variable Names (camelCase)
```cangjie
// Correct
let messageCount: Int64
let processingTime: Duration
let errorHandlingStrategy: ErrorHandlingStrategy

// Incorrect
let MessageCount: Int64
let processing_time: Duration
let error_handling_strategy: ErrorHandlingStrategy
```

### Constant Names (SCREAMING_SNAKE_CASE)
```cangjie
// Correct
let MAX_MESSAGE_SIZE: Int64 = 1024
let DEFAULT_TIMEOUT: Duration = Duration.second * 30
let ACTOR_SYSTEM_NAME: String = "CActor"

// Incorrect
let maxMessageSize: Int64 = 1024
let defaultTimeout: Duration = Duration.second * 30
let actorSystemName: String = "CActor"
```

### Package Names (snake_case)
```cangjie
// Correct
package cactor.core.actor
package cactor.runtime.dispatcher
package cactor.patterns.ask

// Incorrect
package cactor.Core.Actor
package cactor.Runtime.Dispatcher
package cactor.Patterns.Ask
```

## Comment Standards

### Class Comments
```cangjie
/**
 * Core implementation of the Actor system
 *
 * Responsible for managing Actor lifecycle, message routing,
 * and system configuration. Inspired by Akka design patterns,
 * tailored for the Cangjie language.
 *
 * @since 1.0.0
 * @author CActor Team
 */
class CActorSystemImpl <: ActorSystem {
    // Implementation details
}
```

### Function Comments
```cangjie
/**
 * Process a received message
 *
 * @param message The message to process, must not be null
 * @param context Actor context containing sender information
 * @return Message processing result indicating success or failure
 * @throws CActorException Thrown when message processing fails
 */
func receive(message: Message, context: ActorContext): MessageResult {
    // Implementation logic
}
```

### Inline Comments
```cangjie
// Calculate average processing time (in milliseconds)
let avgProcessingTime = totalTime / messageCount

// TODO: Implement a more efficient message routing algorithm
// FIXME: Fix race condition during concurrent access
// NOTE: Using a simplified implementation, production environment needs optimization
```

## Code Structure

### File Organization
```cangjie
/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - File purpose description
 * Detailed explanation of the file's role and design rationale
 */
package cactor.core.actor

// 1. Standard library imports
import std.time.*
import std.collection.*

// 2. CActor core module imports
import cactor.core.message.*
import cactor.core.context.*

// 3. Third-party library imports (if any)

// 4. Type definitions
public enum MessageResult {
    // Enum values
}

// 5. Interface definitions
public interface Actor {
    // Interface methods
}

// 6. Class implementations
public class ActorImpl <: Actor {
    // Class implementation
}
```

### Class Internal Structure
```cangjie
class ExampleActor <: Actor {
    // 1. Constant definitions
    private static let MAX_RETRIES: Int32 = 3

    // 2. Property definitions
    private var messageCount: Int64 = 0
    private let startTime: Int64

    // 3. Constructor
    public init() {
        this.startTime = System.currentTimeMillis()
    }

    // 4. Public properties
    public prop name: String {
        get() { "ExampleActor" }
    }

    // 5. Public methods
    public func receive(message: Message, context: ActorContext): MessageResult {
        // Implementation
    }

    // 6. Private methods
    private func processMessage(message: Message): Unit {
        // Implementation
    }
}
```

## Formatting Standards

### Indentation and Spacing
```cangjie
// Correct - use 4 spaces for indentation
class Actor {
    func receive(message: Message): MessageResult {
        match (message) {
            case ping: PingMessage =>
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}

// Incorrect - using Tab or inconsistent indentation
class Actor {
	func receive(message: Message): MessageResult {
		// Wrong indentation
	}
}
```

### Line Length
```cangjie
// Correct - lines should not exceed 120 characters
let result = processMessage(message, context, additionalParam)

// Correct - break long lines appropriately
let result = processMessage(
    message,
    context,
    additionalParam,
    anotherParam
)

// Incorrect - line too long
let result = processMessage(message, context, additionalParam, anotherParam, yetAnotherParam, andAnotherOne)
```

### Blank Line Usage
```cangjie
class ExampleActor <: Actor {
    private var count: Int64 = 0

    public func receive(message: Message, context: ActorContext): MessageResult {
        count += 1

        match (message) {
            case ping: PingMessage =>
                handlePing(ping)
                MessageResult.Handled

            case pong: PongMessage =>
                handlePong(pong)
                MessageResult.Handled

            case _ =>
                MessageResult.Unhandled
        }
    }

    private func handlePing(ping: PingMessage): Unit {
        // Handle ping message
    }

    private func handlePong(pong: PongMessage): Unit {
        // Handle pong message
    }
}
```

## Error Handling Standards

### Exception Handling
```cangjie
// Correct - specific exception handling
try {
    processMessage(message)
} catch (e: CActorException) {
    // Handle CActor-specific exceptions
    handleCActorError(e)
} catch (e: Exception) {
    // Handle generic exceptions
    handleGenericError(e)
}

// Incorrect - catching all exceptions without handling
try {
    processMessage(message)
} catch (e: Exception) {
    // Empty exception handling
}
```

### Error Returns
```cangjie
// Correct - using Result type
func processMessage(message: Message): Result<Unit, CActorError> {
    if (message.isValid()) {
        // Process message
        Ok(())
    } else {
        Err(CActorError.InvalidMessage("Invalid message format"))
    }
}

// Correct - using Option type
func findActor(name: String): Option<ActorRef> {
    if (actors.containsKey(name)) {
        Some(actors[name])
    } else {
        None
    }
}
```

## Performance Standards

### Avoiding Unnecessary Object Creation
```cangjie
// Correct - reusing objects
class MessageProcessor {
    private let stringBuilder: StringBuilder = StringBuilder()

    func formatMessage(message: Message): String {
        stringBuilder.clear()
        stringBuilder.append("Message: ")
        stringBuilder.append(message.toString())
        stringBuilder.toString()
    }
}

// Incorrect - creating objects frequently
func formatMessage(message: Message): String {
    let sb = StringBuilder()  // Creates new object every time
    sb.append("Message: ")
    sb.append(message.toString())
    sb.toString()
}
```

### Efficient Collection Operations
```cangjie
// Correct - preallocate capacity
let messages = ArrayList<Message>(expectedSize)

// Correct - use appropriate collection types
let actorIndex = HashMap<String, ActorRef>()  // Fast lookup
let messageQueue = ArrayDeque<Message>()      // Efficient queue operations

// Incorrect - using inappropriate collection types
let actorIndex = ArrayList<Pair<String, ActorRef>>()  // Low lookup efficiency
```

## Testing Standards

### Test Naming
```cangjie
// Correct - descriptive test names
@Test
func shouldReturnHandledWhenReceivingValidMessage(): Unit {
    // Test implementation
}

@Test
func shouldThrowExceptionWhenMessageIsNull(): Unit {
    // Test implementation
}

// Incorrect - unclear test names
@Test
func test1(): Unit {
    // Test implementation
}

@Test
func messageTest(): Unit {
    // Test implementation
}
```

### Test Structure
```cangjie
@Test
func shouldProcessMessageCorrectly(): Unit {
    // Arrange - prepare test data
    let actor = TestActor()
    let message = PingMessage("test")
    let context = MockActorContext()

    // Act - execute the test operation
    let result = actor.receive(message, context)

    // Assert - verify the results
    assertEquals(MessageResult.Handled, result)
    assertEquals(1, actor.getProcessedCount())
}
```

## Documentation Standards

### README Files
- Every module should have a README.md file
- Include module description, usage examples, and API documentation links
- Keep content concise and clear

### API Documentation
- All public interfaces must have complete documentation comments
- Include parameter descriptions, return value descriptions, and usage examples
- Note version information and author details

### Changelog
- Record all significant changes
- Organize by version, including new features, fixes, and breaking changes
- Use semantic versioning

## Code Review Checklist

### Pre-commit Checks
- [ ] Code follows naming conventions
- [ ] Necessary comments and documentation added
- [ ] Corresponding unit tests included
- [ ] All existing tests pass
- [ ] Performance has not degraded noticeably
- [ ] No new compiler warnings introduced
- [ ] Error handling standards followed

### Review Focus Areas
- [ ] Code logic is clear and easy to understand
- [ ] No duplicate code
- [ ] Exception handling is complete
- [ ] Resource management is correct
- [ ] Thread safety considerations addressed
- [ ] Boundary conditions handled

## Tool Support

### Formatting Tools
```bash
# Use Cangjie formatting tool (if available)
cj fmt src/

# Check code style
cj lint src/
```

### Static Analysis
```bash
# Run static analysis tools
cj analyze src/

# Check for potential issues
cj check src/
```

---

**Note**: This guide is a living document and will be continuously updated based on project evolution and team feedback. All team members should follow these standards to ensure code quality and consistency.