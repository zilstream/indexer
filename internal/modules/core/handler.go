package core

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// EventHandlerFunc is the function signature for event handlers
type EventHandlerFunc func(ctx context.Context, event *ParsedEvent) error

// BlockHandlerFunc is the function signature for block handlers
type BlockHandlerFunc func(ctx context.Context, block *types.Block) error

// ParsedEvent represents a decoded event log
type ParsedEvent struct {
	// Raw log data
	Log *types.Log
	
	// Event information
	EventName string
	Address   common.Address
	
	// Parsed event data
	Args map[string]interface{}
	
	// Transaction context
	TransactionHash  common.Hash
	TransactionIndex uint
	BlockNumber      uint64
	BlockHash        common.Hash
	LogIndex         uint
	
	// Additional context
	Timestamp *big.Int
}

// EventParser handles parsing of event logs using ABI definitions
type EventParser struct {
	contracts map[common.Address]*abi.ABI
	events    map[common.Hash]*abi.Event // topic0 -> event
}

// NewEventParser creates a new event parser
func NewEventParser() *EventParser {
	return &EventParser{
		contracts: make(map[common.Address]*abi.ABI),
		events:    make(map[common.Hash]*abi.Event),
	}
}

// AddContract adds a contract ABI for parsing
func (p *EventParser) AddContract(address common.Address, contractABI *abi.ABI) {
	p.contracts[address] = contractABI
	
	// Index events by topic hash
	for _, event := range contractABI.Events {
		p.events[event.ID] = &event
	}
}

// ParseEvent parses a log into a ParsedEvent
func (p *EventParser) ParseEvent(log *types.Log) (*ParsedEvent, error) {
	if len(log.Topics) == 0 {
		return nil, ErrInvalidEvent{Reason: "no topics in log"}
	}

	// Find the event by topic0 (event signature)
	eventABI, exists := p.events[log.Topics[0]]
	if !exists {
		return nil, ErrUnknownEvent{Topic: log.Topics[0].Hex()}
	}

	// Special check for Mint event to debug the issue
	if eventABI.Name == "Mint" && len(log.Topics) < 2 {
		return nil, ErrInvalidEvent{Reason: fmt.Sprintf("Mint event has insufficient topics: expected at least 2, got %d", len(log.Topics))}
	}
	
	// Parse the event data
	args := make(map[string]interface{})
	
	// Parse indexed parameters (topics[1:])
	indexedArgs := make([]interface{}, 0)
	topicIndex := 1 // Start from topics[1] since topics[0] is the event signature
	for _, input := range eventABI.Inputs {
		if input.Indexed {
			if topicIndex < len(log.Topics) {
				// Convert topic to appropriate type
				value := p.parseIndexedArg(log.Topics[topicIndex], input.Type)
				args[input.Name] = value
				indexedArgs = append(indexedArgs, value)
				topicIndex++
			}
		}
	}
	
	// Parse non-indexed parameters (data field)
	if len(log.Data) > 0 {
		nonIndexedInputs := make(abi.Arguments, 0)
		for _, input := range eventABI.Inputs {
			if !input.Indexed {
				nonIndexedInputs = append(nonIndexedInputs, input)
			}
		}
		
		if len(nonIndexedInputs) > 0 {
			nonIndexedArgs, err := nonIndexedInputs.Unpack(log.Data)
			if err != nil {
				return nil, ErrEventParsing{Event: eventABI.Name, Err: err}
			}
			
			// Map non-indexed args to parameter names
			for i, input := range nonIndexedInputs {
				if i < len(nonIndexedArgs) {
					args[input.Name] = nonIndexedArgs[i]
				}
			}
		}
	}
	
	return &ParsedEvent{
		Log:              log,
		EventName:        eventABI.Name,
		Address:          log.Address,
		Args:             args,
		TransactionHash:  log.TxHash,
		TransactionIndex: log.TxIndex,
		BlockNumber:      log.BlockNumber,
		BlockHash:        log.BlockHash,
		LogIndex:         log.Index,
	}, nil
}

// parseIndexedArg converts a topic hash to the appropriate Go type
func (p *EventParser) parseIndexedArg(topic common.Hash, argType abi.Type) interface{} {
	switch argType.T {
	case abi.AddressTy:
		return common.HexToAddress(topic.Hex())
	case abi.IntTy, abi.UintTy:
		return new(big.Int).SetBytes(topic.Bytes())
	case abi.BoolTy:
		return topic.Big().Cmp(common.Big0) != 0
	case abi.BytesTy, abi.FixedBytesTy:
		return topic.Bytes()
	case abi.StringTy, abi.HashTy:
		return topic.Hex()
	default:
		// For complex types, return the raw hash
		return topic.Hex()
	}
}

// ParseEventSignature parses an event signature string (e.g., "Transfer(address,address,uint256)")
func ParseEventSignature(sig string) (*abi.Event, error) {
	// Extract event name and parameters
	parenIdx := strings.Index(sig, "(")
	if parenIdx == -1 {
		return nil, ErrInvalidEventSignature{Signature: sig}
	}
	
	name := sig[:parenIdx]
	params := sig[parenIdx+1 : len(sig)-1] // Remove parentheses
	
	// Parse parameters
	var inputs abi.Arguments
	if params != "" {
		paramList := strings.Split(params, ",")
		for i, param := range paramList {
			param = strings.TrimSpace(param)
			
			// Check for indexed keyword
			indexed := false
			if strings.HasPrefix(param, "indexed ") {
				indexed = true
				param = strings.TrimPrefix(param, "indexed ")
			}
			
			// Parse type
			argType, err := abi.NewType(param, "", nil)
			if err != nil {
				return nil, ErrInvalidEventSignature{Signature: sig}
			}
			
			inputs = append(inputs, abi.Argument{
				Name:    "", // Will be filled by specific implementations
				Type:    argType,
				Indexed: indexed,
			})
			
			// Set a default name if none provided
			if len(inputs) > 0 {
				inputs[i].Name = inputs[i].Type.String() + "_" + string(rune(i))
			}
		}
	}
	
	event := abi.NewEvent(name, name, false, inputs)
	return &event, nil
}

// Error types
type ErrInvalidEvent struct {
	Reason string
}

func (e ErrInvalidEvent) Error() string {
	return "invalid event: " + e.Reason
}

type ErrUnknownEvent struct {
	Topic string
}

func (e ErrUnknownEvent) Error() string {
	return "unknown event topic: " + e.Topic
}

type ErrEventParsing struct {
	Event string
	Err   error
}

func (e ErrEventParsing) Error() string {
	return "failed to parse event " + e.Event + ": " + e.Err.Error()
}

type ErrInvalidEventSignature struct {
	Signature string
}

func (e ErrInvalidEventSignature) Error() string {
	return "invalid event signature: " + e.Signature
}