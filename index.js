// index.js
require('dotenv').config();
const express = require('express');
const { Web3 } = require('web3');
const HDWalletProvider = require('@truffle/hdwallet-provider');
const BigNumber = require('bignumber.js');

const app = express();

// Middleware
app.use(express.json());

// WEB3 CONFIG - Add multiple fallback RPC URLs
const RPC_URLS = [
    process.env.RPC_URL1,
    process.env.RPC_URL2,
    process.env.RPC_URL3,
    process.env.RPC_URL4,
    process.env.RPC_URL5,
].filter(url => url);

let web3;
let currentRpcIndex = 0;

// Initialize Web3 with proper error handling
function initializeWeb3() {
    try {
        if (!process.env.PRIVATE_KEY) {
            throw new Error('PRIVATE_KEY environment variable is required');
        }
        
        if (!RPC_URLS.length) {
            throw new Error('At least one RPC_URL is required');
        }
        
        const provider = new HDWalletProvider(process.env.PRIVATE_KEY, RPC_URLS[currentRpcIndex]);
        web3 = new Web3(provider);
        console.log(`Initialized Web3 with RPC: ${RPC_URLS[currentRpcIndex]}`);
        return true;
    } catch (error) {
        console.error('Failed to initialize Web3:', error.message);
        return false;
    }
}

// Function to switch RPC provider
function switchRpcProvider() {
    if(currentRpcIndex < RPC_URLS.length - 1) {
        currentRpcIndex = currentRpcIndex + 1;
    } else {
        currentRpcIndex = 0;
    }
    
    try {
        const newProvider = new HDWalletProvider(process.env.PRIVATE_KEY, RPC_URLS[currentRpcIndex]);
        web3.setProvider(newProvider);
        console.log(`Switched to RPC: ${RPC_URLS[currentRpcIndex]}`);
        return true;
    } catch (error) {
        console.error('Failed to switch RPC provider:', error.message);
        return false;
    }
}

// UniswapV2_ABI for Polygon POS
const UNISWAPV2_ABI = [{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"sync","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"}];

let pairs = [
    ['0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619', '0xc2132D05D31c914a87C6611C10748AEb04B58e8F']
];

let pair_names = ['WETH/USDT'];
let minProfitAboveGas = ['2500000000000'];

const UNISWAPV2_FACTORY_ABI = [{"inputs":[{"internalType":"address","name":"_feeToSetter","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":false,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"uint256","name":"","type":"uint256"}],"name":"PairCreated","type":"event"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"allPairs","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"allPairsLength","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"}],"name":"createPair","outputs":[{"internalType":"address","name":"pair","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"feeTo","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeToSetter","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"getPair","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"migrator","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"pairCodeHash","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"_feeTo","type":"address"}],"name":"setFeeTo","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_feeToSetter","type":"address"}],"name":"setFeeToSetter","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_migrator","type":"address"}],"name":"setMigrator","outputs":[],"stateMutability":"nonpayable","type":"function"}];

let factories_address = ['0x9e5A52f57b3038F1B8EeE45F28b3C1967e22799C', '0xc35DADB65012eC5796536bD9864eD8773aBc74C4', '0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32'];
let factories_name = ['UniswapV2', 'Sushiswap', 'Quickswap'];

const BONJOURV3_ABI = [{"type":"constructor","inputs":[{"name":"keeper","type":"address","internalType":"address"}],"stateMutability":"nonpayable"},{"type":"function","name":"owner","inputs":[],"outputs":[{"name":"","type":"address","internalType":"address"}],"stateMutability":"view"},{"type":"function","name":"performUpkeep","inputs":[{"name":"token0","type":"address","internalType":"address"},{"name":"token1","type":"address","internalType":"address"},{"name":"inputDX","type":"uint256","internalType":"uint256"},{"name":"poolA","type":"address","internalType":"address"},{"name":"poolB","type":"address","internalType":"address"},{"name":"uniV3Position","type":"uint256","internalType":"uint256"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"renounceOwnership","inputs":[],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"s_keeper","inputs":[],"outputs":[{"name":"","type":"address","internalType":"address"}],"stateMutability":"view"},{"type":"function","name":"setKeeper","inputs":[{"name":"newKeeper","type":"address","internalType":"address"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"transferOwnership","inputs":[{"name":"newOwner","type":"address","internalType":"address"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"uniswapV2Call","inputs":[{"name":"","type":"address","internalType":"address"},{"name":"","type":"uint256","internalType":"uint256"},{"name":"amount1","type":"uint256","internalType":"uint256"},{"name":"data","type":"bytes","internalType":"bytes"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"uniswapV3SwapCallback","inputs":[{"name":"amount0Delta","type":"int256","internalType":"int256"},{"name":"amount1Delta","type":"int256","internalType":"int256"},{"name":"data","type":"bytes","internalType":"bytes"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"event","name":"OwnershipTransferred","inputs":[{"name":"previousOwner","type":"address","indexed":true,"internalType":"address"},{"name":"newOwner","type":"address","indexed":true,"internalType":"address"}],"anonymous":false},{"type":"error","name":"Bonjour__EthWithdrawFailed","inputs":[]},{"type":"error","name":"Bonjour__NotEnoughEthInContract","inputs":[]},{"type":"error","name":"Bonjour__NotEnoughEthSent","inputs":[]},{"type":"error","name":"OwnableInvalidOwner","inputs":[{"name":"owner","type":"address","internalType":"address"}]},{"type":"error","name":"OwnableUnauthorizedAccount","inputs":[{"name":"account","type":"address","internalType":"address"}]},{"type":"error","name":"SafeERC20FailedOperation","inputs":[{"name":"token","type":"address","internalType":"address"}]}];

const bonjour_address = '0xAA6fE0D45102d65eD0e5e1fC00709f864c86773a';
let bonjourContract;

// Retry function with exponential backoff
async function withRetry(operation, maxRetries = 5, operationName = 'operation') {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            const errorMessage = error ? error.message : 'Unknown error occurred';
            console.error(`Attempt ${attempt}/${maxRetries} failed for ${operationName}:`, errorMessage);
            
            if (attempt === maxRetries) {
                throw error;
            }
            
            const delayMs = Math.pow(2, attempt) * 100;
            console.log(`Waiting ${delayMs}ms before retry...`);
            await new Promise(resolve => setTimeout(resolve, delayMs));
            
            if (error && error.message && (error.message.includes('ETIMEDOUT') || error.message.includes('TIMEOUT'))) {
                switchRpcProvider();
            }
        }
    }
}

// Enhanced factory call with better error handling
async function callFactoryWithRetry(factoryContract, methodName, args, factoryName) {
    return withRetry(async () => {
        try {
            const result = await factoryContract.methods[methodName](...args).call();
            return result;
        } catch (error) {
            if (error.message.includes('revert') || error.message.includes('invalid opcode')) {
                console.log(`Contract might not exist or be deployed on ${factoryName}`);
                return null;
            }
            throw error;
        }
    }, 5, `${factoryName} ${methodName}`);
}

async function getSqrtPricesAndLiquidities(inputPair) {
    const sqrtPricesAndLiquidities = [];
    const pools = [];
    
    // Process factories sequentially to avoid overloading
    for (let j = 0; j < factories_name.length; j++) {
        const name = factories_name[j];
        const address = factories_address[j];
        
        try {
            const factoryContract = new web3.eth.Contract(UNISWAPV2_FACTORY_ABI, address);
            
            // Get pair address with retry
            const swapPool_address = await callFactoryWithRetry(
                factoryContract, 
                'getPair', 
                [inputPair[0], inputPair[1]],
                name
            );
            
            if (!swapPool_address || swapPool_address === '0x0000000000000000000000000000000000000000') {
                continue;
            }
            
            // Get reserves with retry
            const swapPoolContract = new web3.eth.Contract(UNISWAPV2_ABI, swapPool_address);
            const reserves = await withRetry(
                () => swapPoolContract.methods.getReserves().call(),
                5,
                `${name} getReserves`
            );
            
            let reserve0 = new BigNumber(reserves._reserve0.toString());
            let reserve1 = new BigNumber(reserves._reserve1.toString());
            let sqrtPrice = reserve1.dividedBy(reserve0).squareRoot();
            let liquidity = reserve0.multipliedBy(reserve1).squareRoot();
            
            sqrtPricesAndLiquidities.push([sqrtPrice, liquidity]);
            pools.push(swapPool_address);
            
            // Add delay between factory calls to avoid rate limiting
            await new Promise(resolve => setTimeout(resolve, 100));
            
        } catch (error) {
            console.error(`Error with factory ${name}:`, error.message);
            continue;
        }
    }
    
    return { sqrtPricesAndLiquidities, pools };
}

async function calculateArbitrageProfits(sqrtPricesAndLiquidities, pools, minProfitAboveGas) {    
    const profits = [];
    const pools_pair = [];
    const inputDxs = [];
    
    try {
        if (sqrtPricesAndLiquidities.length < 2) {
            return { profits, pools_pair, inputDxs };
        }
        
        for (let i = sqrtPricesAndLiquidities.length - 1; i > 0; i--) {
            for (let k = i - 1; k >= 0; k--) {
                const sqrtPriceI = sqrtPricesAndLiquidities[i][0]
                const liquidityI = sqrtPricesAndLiquidities[i][1]
                const sqrtPriceK = sqrtPricesAndLiquidities[k][0]
                const liquidityK = sqrtPricesAndLiquidities[k][1]
                
                const numerator = liquidityI.multipliedBy(liquidityK).multipliedBy(sqrtPriceI.minus(sqrtPriceK));
                
                let inputDx, outputDx, profit;
                
                if (sqrtPriceI.isGreaterThan(sqrtPriceK)) {
                    const inputDenominator = new BigNumber('0.997')
                        .multipliedBy(liquidityI)
                        .multipliedBy(sqrtPriceI.pow(2))
                        .plus(liquidityK.multipliedBy(sqrtPriceI).multipliedBy(sqrtPriceK));
                    
                    const outputDenominator = liquidityK
                        .multipliedBy(sqrtPriceK.pow(2))
                        .plus(
                            new BigNumber('0.997')
                                .multipliedBy(liquidityI)
                                .multipliedBy(sqrtPriceI)
                                .multipliedBy(sqrtPriceK)
                        );
                    
                    inputDx = numerator.dividedBy(inputDenominator.multipliedBy(new BigNumber('0.997')));
                    outputDx = numerator.multipliedBy(new BigNumber('0.997')).dividedBy(outputDenominator);
                    profit = outputDx.minus(inputDx);
                    
                    if (profit.isGreaterThan(minProfitAboveGas)) {
                        profits.push(profit);
                        pools_pair.push([pools[i], pools[k]]);
                        inputDxs.push(inputDx);
                    }
                } else {
                    const inputDenominator = new BigNumber('0.997')
                        .multipliedBy(liquidityK)
                        .multipliedBy(sqrtPriceK.pow(2))
                        .plus(liquidityI.multipliedBy(sqrtPriceI).multipliedBy(sqrtPriceK));
                    
                    const outputDenominator = liquidityI
                        .multipliedBy(sqrtPriceI.pow(2))
                        .plus(
                            new BigNumber('0.997')
                                .multipliedBy(liquidityK)
                                .multipliedBy(sqrtPriceI)
                                .multipliedBy(sqrtPriceK)
                        );
                    
                    inputDx = new BigNumber('-1').multipliedBy(numerator.dividedBy(inputDenominator.multipliedBy(new BigNumber('0.998'))));
                    outputDx = new BigNumber('-1').multipliedBy(numerator.multipliedBy(new BigNumber('0.997')).dividedBy(outputDenominator));
                    profit = outputDx.minus(inputDx);
                    
                    if (profit.isGreaterThan(minProfitAboveGas)) {
                        profits.push(profit);
                        pools_pair.push([pools[k], pools[i]]);
                        inputDxs.push(inputDx);
                    }
                }
            }
        }
        
        return { profits, pools_pair, inputDxs };
        
    } catch (error) {
        console.error('Error in calculateArbitrageProfits:', error);
        return { profits, pools_pair, inputDxs };
    }
}

async function debugTransaction(pairIndex, inputAmount, poolA, poolB) {
    try {
        console.log('Debugging transaction parameters:');
        console.log('Token 0:', pairs[pairIndex][0]);
        console.log('Token 1:', pairs[pairIndex][1]);
        console.log('Input amount:', inputAmount.toString());
        console.log('Pool A:', poolA);
        console.log('Pool B:', poolB);
        
        try {
            const gasEstimate = await bonjourContract.methods.performUpkeep(
                pairs[pairIndex][0],
                pairs[pairIndex][1],
                inputAmount.toString(),
                poolA,
                poolB,
                '3'
            ).estimateGas({ 
                from: process.env.ACCOUNT,
                gas: 500000
            });
            
            console.log('Gas estimate:', gasEstimate);
            return true;
            
        } catch (estimateError) {
            const errorMessage = estimateError.message || 'Unknown gas estimation error';
            console.error('Gas estimation failed:', errorMessage);
            
            if (estimateError.data) {
                console.error('Estimation error data:', estimateError.data);
            }
            
            if (errorMessage.includes('revert') || errorMessage.includes('execution reverted')) {
                console.error('Transaction would revert - check contract logic or parameters');
            } else if (errorMessage.includes('gas')) {
                console.error('Gas-related error - may need higher gas limit');
            }
            
            return false;
        }
    } catch (debugError) {
        const errorMsg = debugError.message || 'Unknown debug error';
        console.error('Debug error:', errorMsg);
        return false;
    }
}

async function validatePool(poolAddress) {
    try {
        const code = await web3.eth.getCode(poolAddress);
        return code !== '0x' && code !== '0x0';
    } catch (error) {
        console.error('Error checking pool validity:', error.message);
        return false;
    }
}

async function executeArbitrage(pairIndex, profits, pools_pair, inputDxs) {
    if (profits.length === 0) return;
    
    let maxProfit = profits[0];
    let maxProfitIndex = 0;
    
    for (let i = 1; i < profits.length; i++) {
        if (profits[i].isGreaterThan(maxProfit)) {
            maxProfit = profits[i];
            maxProfitIndex = i;
        }
    }
    
    try {
        console.log(`Executing arbitrage for pair ${pair_names[pairIndex]} with profit: ${maxProfit.toString()}`);
        
        const inputAmount = inputDxs[maxProfitIndex].integerValue(BigNumber.ROUND_DOWN);
        
        if (inputAmount.isLessThanOrEqualTo(0) || !inputAmount.isFinite()) {
            console.error('Invalid input amount:', inputAmount.toString());
            return;
        }
        
        const poolA = pools_pair[maxProfitIndex][0];
        const poolB = pools_pair[maxProfitIndex][1];
        
        const poolAValid = await validatePool(poolA);
        const poolBValid = await validatePool(poolB);
        
        if (!poolAValid || !poolBValid) {
            console.error('Invalid pool addresses detected');
            if (!poolAValid) console.error('Pool A invalid:', poolA);
            if (!poolBValid) console.error('Pool B invalid:', poolB);
            return;
        }
        
        const debugResult = await debugTransaction(
            pairIndex,
            inputAmount,
            poolA,
            poolB
        );
        
        if (!debugResult) {
            console.error('Transaction debugging failed, skipping execution');
            return;
        }
        
        const receipt = await withRetry(async () => {
            const gasPrice = await web3.eth.getGasPrice();
            console.log('Using gas price:', web3.utils.fromWei(gasPrice, 'gwei'), 'gwei');
            
            return await bonjourContract.methods.performUpkeep(
                pairs[pairIndex][0],
                pairs[pairIndex][1],
                inputAmount.toString(),
                poolA,
                poolB,
                '3'
            ).send({
                from: process.env.ACCOUNT,
                gas: 800000,
                gasPrice: gasPrice
            });
        }, 3, 'performUpkeep');
        
        console.log("Transaction successful! Receipt:", receipt.transactionHash);
        return receipt.transactionHash;
        
    } catch (error) {
        const errorMsg = error ? error.message : 'Unknown error during arbitrage execution';
        console.error('Failed to execute arbitrage:', errorMsg);
        
        if (error) {
            console.error('Error code:', error.code);
            console.error('Error data:', error.data);
            
            if (error.code === -32603) {
                console.error('Internal RPC error - may need to switch providers');
                switchRpcProvider();
            }
        }
        
        throw error;
    }
}

async function checkPairForArbitrage(pairIndex) {
    try {
        const result = await withRetry(
            () => getSqrtPricesAndLiquidities(pairs[pairIndex]),
            5,
            `getSqrtPricesAndLiquidities for ${pair_names[pairIndex]}`
        );
        
        if (!result || result.sqrtPricesAndLiquidities.length < 2) {
            return { success: false, message: `Not enough pools found for ${pair_names[pairIndex]}` };
        }
        
        const nextResult = await calculateArbitrageProfits(
            result.sqrtPricesAndLiquidities,
            result.pools,
            new BigNumber(minProfitAboveGas[pairIndex])
        );
        
        if (nextResult && nextResult.profits.length > 0) {
            const txHash = await executeArbitrage(pairIndex, nextResult.profits, nextResult.pools_pair, nextResult.inputDxs);
            return { success: true, txHash, profits: nextResult.profits.length };
        } else {
            return { success: false, message: `No profitable arbitrage opportunities for ${pair_names[pairIndex]}` };
        }
        
    } catch (error) {
        const errorMsg = error ? error.message : 'Unknown error checking pair';
        console.error(`Error checking pair ${pair_names[pairIndex]}:`, errorMsg);
        return { success: false, error: errorMsg };
    }
}

async function checkBalancesAndArbitrage() {
    console.log('=== Starting arbitrage check cycle ===');
    const results = [];
    
    // Process pairs sequentially to avoid overloading
    for (let j = 0; j < pairs.length; j++) {
        const result = await checkPairForArbitrage(j);
        results.push({
            pair: pair_names[j],
            ...result
        });
        
        // Add delay between pairs to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    
    console.log('=== Completed arbitrage check cycle ===');
    return results;
}

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        currentRpc: RPC_URLS[currentRpcIndex],
        timestamp: new Date().toISOString()
    });
});

// Main arbitrage endpoint
app.get('/index', async (req, res) => {
    try {
        // Initialize Web3 if not already initialized
        if (!web3 && !initializeWeb3()) {
            return res.status(500).json({ error: 'Failed to initialize Web3' });
        }
        
        // Initialize bonjour contract
        bonjourContract = new web3.eth.Contract(BONJOURV3_ABI, bonjour_address);
        
        const results = await checkBalancesAndArbitrage();
        res.json({ success: true, results });
    } catch (error) {
        console.error('Error in arbitrage endpoint:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Vercel serverless function handler
module.exports = app;