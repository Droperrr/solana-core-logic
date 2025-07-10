const anchor = require('@coral-xyz/anchor');
const provider = anchor.AnchorProvider.env();
const programId = new anchor.web3.PublicKey('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4');
const program = new anchor.Program(programId, null, provider);
const idl = program.idl;
console.log(JSON.stringify(idl, null, 2));