(()=>{var au,Cu,Yu={297:(te,je,ze)=>{te.exports=(function R(M,S,h){function f(w,z){if(!S[w]){if(!M[w]){if(r)return r(w,!0);var k=new Error("Cannot find module '"+w+"'");throw k.code="MODULE_NOT_FOUND",k}var _=S[w]={exports:{}};M[w][0].call(_.exports,(function(o){return f(M[w][1][o]||o)}),_,_.exports,R,M,S,h)}return S[w].exports}for(var r=void 0,c=0;c<h.length;c++)f(h[c]);return f})({1:[function(R,M,S){"use strict";var h=R("./utils"),f=R("./support"),r="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";S.encode=function(c){for(var w,z,k,_,o,v,i,m=[],d=0,x=c.length,E=x,A=h.getTypeOf(c)!=="string";d<c.length;)E=x-d,k=A?(w=c[d++],z=d<x?c[d++]:0,d<x?c[d++]:0):(w=c.charCodeAt(d++),z=d<x?c.charCodeAt(d++):0,d<x?c.charCodeAt(d++):0),_=w>>2,o=(3&w)<<4|z>>4,v=1<E?(15&z)<<2|k>>6:64,i=2<E?63&k:64,m.push(r.charAt(_)+r.charAt(o)+r.charAt(v)+r.charAt(i));return m.join("")},S.decode=function(c){var w,z,k,_,o,v,i=0,m=0,d="data:";if(c.substr(0,d.length)===d)throw new Error("Invalid base64 input, it looks like a data url.");var x,E=3*(c=c.replace(/[^A-Za-z0-9+/=]/g,"")).length/4;if(c.charAt(c.length-1)===r.charAt(64)&&E--,c.charAt(c.length-2)===r.charAt(64)&&E--,E%1!=0)throw new Error("Invalid base64 input, bad content length.");for(x=f.uint8array?new Uint8Array(0|E):new Array(0|E);i<c.length;)w=r.indexOf(c.charAt(i++))<<2|(_=r.indexOf(c.charAt(i++)))>>4,z=(15&_)<<4|(o=r.indexOf(c.charAt(i++)))>>2,k=(3&o)<<6|(v=r.indexOf(c.charAt(i++))),x[m++]=w,o!==64&&(x[m++]=z),v!==64&&(x[m++]=k);return x}},{"./support":30,"./utils":32}],2:[function(R,M,S){"use strict";var h=R("./external"),f=R("./stream/DataWorker"),r=R("./stream/Crc32Probe"),c=R("./stream/DataLengthProbe");function w(z,k,_,o,v){this.compressedSize=z,this.uncompressedSize=k,this.crc32=_,this.compression=o,this.compressedContent=v}w.prototype={getContentWorker:function(){var z=new f(h.Promise.resolve(this.compressedContent)).pipe(this.compression.uncompressWorker()).pipe(new c("data_length")),k=this;return z.on("end",(function(){if(this.streamInfo.data_length!==k.uncompressedSize)throw new Error("Bug : uncompressed data size mismatch")})),z},getCompressedWorker:function(){return new f(h.Promise.resolve(this.compressedContent)).withStreamInfo("compressedSize",this.compressedSize).withStreamInfo("uncompressedSize",this.uncompressedSize).withStreamInfo("crc32",this.crc32).withStreamInfo("compression",this.compression)}},w.createWorkerFrom=function(z,k,_){return z.pipe(new r).pipe(new c("uncompressedSize")).pipe(k.compressWorker(_)).pipe(new c("compressedSize")).withStreamInfo("compression",k)},M.exports=w},{"./external":6,"./stream/Crc32Probe":25,"./stream/DataLengthProbe":26,"./stream/DataWorker":27}],3:[function(R,M,S){"use strict";var h=R("./stream/GenericWorker");S.STORE={magic:"\0\0",compressWorker:function(){return new h("STORE compression")},uncompressWorker:function(){return new h("STORE decompression")}},S.DEFLATE=R("./flate")},{"./flate":7,"./stream/GenericWorker":28}],4:[function(R,M,S){"use strict";var h=R("./utils"),f=(function(){for(var r,c=[],w=0;w<256;w++){r=w;for(var z=0;z<8;z++)r=1&r?3988292384^r>>>1:r>>>1;c[w]=r}return c})();M.exports=function(r,c){return r!==void 0&&r.length?h.getTypeOf(r)!=="string"?(function(w,z,k,_){var o=f,v=_+k;w^=-1;for(var i=_;i<v;i++)w=w>>>8^o[255&(w^z[i])];return-1^w})(0|c,r,r.length,0):(function(w,z,k,_){var o=f,v=_+k;w^=-1;for(var i=_;i<v;i++)w=w>>>8^o[255&(w^z.charCodeAt(i))];return-1^w})(0|c,r,r.length,0):0}},{"./utils":32}],5:[function(R,M,S){"use strict";S.base64=!1,S.binary=!1,S.dir=!1,S.createFolders=!0,S.date=null,S.compression=null,S.compressionOptions=null,S.comment=null,S.unixPermissions=null,S.dosPermissions=null},{}],6:[function(R,M,S){"use strict";var h=null;h=typeof Promise!="undefined"?Promise:R("lie"),M.exports={Promise:h}},{lie:37}],7:[function(R,M,S){"use strict";var h=typeof Uint8Array!="undefined"&&typeof Uint16Array!="undefined"&&typeof Uint32Array!="undefined",f=R("pako"),r=R("./utils"),c=R("./stream/GenericWorker"),w=h?"uint8array":"array";function z(k,_){c.call(this,"FlateWorker/"+k),this._pako=null,this._pakoAction=k,this._pakoOptions=_,this.meta={}}S.magic="\b\0",r.inherits(z,c),z.prototype.processChunk=function(k){this.meta=k.meta,this._pako===null&&this._createPako(),this._pako.push(r.transformTo(w,k.data),!1)},z.prototype.flush=function(){c.prototype.flush.call(this),this._pako===null&&this._createPako(),this._pako.push([],!0)},z.prototype.cleanUp=function(){c.prototype.cleanUp.call(this),this._pako=null},z.prototype._createPako=function(){this._pako=new f[this._pakoAction]({raw:!0,level:this._pakoOptions.level||-1});var k=this;this._pako.onData=function(_){k.push({data:_,meta:k.meta})}},S.compressWorker=function(k){return new z("Deflate",k)},S.uncompressWorker=function(){return new z("Inflate",{})}},{"./stream/GenericWorker":28,"./utils":32,pako:38}],8:[function(R,M,S){"use strict";function h(o,v){var i,m="";for(i=0;i<v;i++)m+=String.fromCharCode(255&o),o>>>=8;return m}function f(o,v,i,m,d,x){var E,A,C=o.file,$=o.compression,T=x!==w.utf8encode,H=r.transformTo("string",x(C.name)),P=r.transformTo("string",w.utf8encode(C.name)),Y=C.comment,ce=r.transformTo("string",x(Y)),y=r.transformTo("string",w.utf8encode(Y)),B=P.length!==C.name.length,t=y.length!==Y.length,N="",pe="",Z="",ge=C.dir,J=C.date,be={crc32:0,compressedSize:0,uncompressedSize:0};v&&!i||(be.crc32=o.crc32,be.compressedSize=o.compressedSize,be.uncompressedSize=o.uncompressedSize);var F=0;v&&(F|=8),T||!B&&!t||(F|=2048);var O=0,le=0;ge&&(O|=16),d==="UNIX"?(le=798,O|=(function(X,Oe){var Me=X;return X||(Me=Oe?16893:33204),(65535&Me)<<16})(C.unixPermissions,ge)):(le=20,O|=(function(X){return 63&(X||0)})(C.dosPermissions)),E=J.getUTCHours(),E<<=6,E|=J.getUTCMinutes(),E<<=5,E|=J.getUTCSeconds()/2,A=J.getUTCFullYear()-1980,A<<=4,A|=J.getUTCMonth()+1,A<<=5,A|=J.getUTCDate(),B&&(pe=h(1,1)+h(z(H),4)+P,N+="up"+h(pe.length,2)+pe),t&&(Z=h(1,1)+h(z(ce),4)+y,N+="uc"+h(Z.length,2)+Z);var ee="";return ee+=`
\0`,ee+=h(F,2),ee+=$.magic,ee+=h(E,2),ee+=h(A,2),ee+=h(be.crc32,4),ee+=h(be.compressedSize,4),ee+=h(be.uncompressedSize,4),ee+=h(H.length,2),ee+=h(N.length,2),{fileRecord:k.LOCAL_FILE_HEADER+ee+H+N,dirRecord:k.CENTRAL_FILE_HEADER+h(le,2)+ee+h(ce.length,2)+"\0\0\0\0"+h(O,4)+h(m,4)+H+N+ce}}var r=R("../utils"),c=R("../stream/GenericWorker"),w=R("../utf8"),z=R("../crc32"),k=R("../signature");function _(o,v,i,m){c.call(this,"ZipFileWorker"),this.bytesWritten=0,this.zipComment=v,this.zipPlatform=i,this.encodeFileName=m,this.streamFiles=o,this.accumulate=!1,this.contentBuffer=[],this.dirRecords=[],this.currentSourceOffset=0,this.entriesCount=0,this.currentFile=null,this._sources=[]}r.inherits(_,c),_.prototype.push=function(o){var v=o.meta.percent||0,i=this.entriesCount,m=this._sources.length;this.accumulate?this.contentBuffer.push(o):(this.bytesWritten+=o.data.length,c.prototype.push.call(this,{data:o.data,meta:{currentFile:this.currentFile,percent:i?(v+100*(i-m-1))/i:100}}))},_.prototype.openedSource=function(o){this.currentSourceOffset=this.bytesWritten,this.currentFile=o.file.name;var v=this.streamFiles&&!o.file.dir;if(v){var i=f(o,v,!1,this.currentSourceOffset,this.zipPlatform,this.encodeFileName);this.push({data:i.fileRecord,meta:{percent:0}})}else this.accumulate=!0},_.prototype.closedSource=function(o){this.accumulate=!1;var v=this.streamFiles&&!o.file.dir,i=f(o,v,!0,this.currentSourceOffset,this.zipPlatform,this.encodeFileName);if(this.dirRecords.push(i.dirRecord),v)this.push({data:(function(m){return k.DATA_DESCRIPTOR+h(m.crc32,4)+h(m.compressedSize,4)+h(m.uncompressedSize,4)})(o),meta:{percent:100}});else for(this.push({data:i.fileRecord,meta:{percent:0}});this.contentBuffer.length;)this.push(this.contentBuffer.shift());this.currentFile=null},_.prototype.flush=function(){for(var o=this.bytesWritten,v=0;v<this.dirRecords.length;v++)this.push({data:this.dirRecords[v],meta:{percent:100}});var i=this.bytesWritten-o,m=(function(d,x,E,A,C){var $=r.transformTo("string",C(A));return k.CENTRAL_DIRECTORY_END+"\0\0\0\0"+h(d,2)+h(d,2)+h(x,4)+h(E,4)+h($.length,2)+$})(this.dirRecords.length,i,o,this.zipComment,this.encodeFileName);this.push({data:m,meta:{percent:100}})},_.prototype.prepareNextSource=function(){this.previous=this._sources.shift(),this.openedSource(this.previous.streamInfo),this.isPaused?this.previous.pause():this.previous.resume()},_.prototype.registerPrevious=function(o){this._sources.push(o);var v=this;return o.on("data",(function(i){v.processChunk(i)})),o.on("end",(function(){v.closedSource(v.previous.streamInfo),v._sources.length?v.prepareNextSource():v.end()})),o.on("error",(function(i){v.error(i)})),this},_.prototype.resume=function(){return!!c.prototype.resume.call(this)&&(!this.previous&&this._sources.length?(this.prepareNextSource(),!0):this.previous||this._sources.length||this.generatedError?void 0:(this.end(),!0))},_.prototype.error=function(o){var v=this._sources;if(!c.prototype.error.call(this,o))return!1;for(var i=0;i<v.length;i++)try{v[i].error(o)}catch(m){}return!0},_.prototype.lock=function(){c.prototype.lock.call(this);for(var o=this._sources,v=0;v<o.length;v++)o[v].lock()},M.exports=_},{"../crc32":4,"../signature":23,"../stream/GenericWorker":28,"../utf8":31,"../utils":32}],9:[function(R,M,S){"use strict";var h=R("../compressions"),f=R("./ZipFileWorker");S.generateWorker=function(r,c,w){var z=new f(c.streamFiles,w,c.platform,c.encodeFileName),k=0;try{r.forEach((function(_,o){k++;var v=(function(x,E){var A=x||E,C=h[A];if(!C)throw new Error(A+" is not a valid compression method !");return C})(o.options.compression,c.compression),i=o.options.compressionOptions||c.compressionOptions||{},m=o.dir,d=o.date;o._compressWorker(v,i).withStreamInfo("file",{name:_,dir:m,date:d,comment:o.comment||"",unixPermissions:o.unixPermissions,dosPermissions:o.dosPermissions}).pipe(z)})),z.entriesCount=k}catch(_){z.error(_)}return z}},{"../compressions":3,"./ZipFileWorker":8}],10:[function(R,M,S){"use strict";function h(){if(!(this instanceof h))return new h;if(arguments.length)throw new Error("The constructor with parameters has been removed in JSZip 3.0, please check the upgrade guide.");this.files=Object.create(null),this.comment=null,this.root="",this.clone=function(){var f=new h;for(var r in this)typeof this[r]!="function"&&(f[r]=this[r]);return f}}(h.prototype=R("./object")).loadAsync=R("./load"),h.support=R("./support"),h.defaults=R("./defaults"),h.version="3.10.1",h.loadAsync=function(f,r){return new h().loadAsync(f,r)},h.external=R("./external"),M.exports=h},{"./defaults":5,"./external":6,"./load":11,"./object":15,"./support":30}],11:[function(R,M,S){"use strict";var h=R("./utils"),f=R("./external"),r=R("./utf8"),c=R("./zipEntries"),w=R("./stream/Crc32Probe"),z=R("./nodejsUtils");function k(_){return new f.Promise((function(o,v){var i=_.decompressed.getContentWorker().pipe(new w);i.on("error",(function(m){v(m)})).on("end",(function(){i.streamInfo.crc32!==_.decompressed.crc32?v(new Error("Corrupted zip : CRC32 mismatch")):o()})).resume()}))}M.exports=function(_,o){var v=this;return o=h.extend(o||{},{base64:!1,checkCRC32:!1,optimizedBinaryString:!1,createFolders:!1,decodeFileName:r.utf8decode}),z.isNode&&z.isStream(_)?f.Promise.reject(new Error("JSZip can't accept a stream when loading a zip file.")):h.prepareContent("the loaded zip file",_,!0,o.optimizedBinaryString,o.base64).then((function(i){var m=new c(o);return m.load(i),m})).then((function(i){var m=[f.Promise.resolve(i)],d=i.files;if(o.checkCRC32)for(var x=0;x<d.length;x++)m.push(k(d[x]));return f.Promise.all(m)})).then((function(i){for(var m=i.shift(),d=m.files,x=0;x<d.length;x++){var E=d[x],A=E.fileNameStr,C=h.resolve(E.fileNameStr);v.file(C,E.decompressed,{binary:!0,optimizedBinaryString:!0,date:E.date,dir:E.dir,comment:E.fileCommentStr.length?E.fileCommentStr:null,unixPermissions:E.unixPermissions,dosPermissions:E.dosPermissions,createFolders:o.createFolders}),E.dir||(v.file(C).unsafeOriginalName=A)}return m.zipComment.length&&(v.comment=m.zipComment),v}))}},{"./external":6,"./nodejsUtils":14,"./stream/Crc32Probe":25,"./utf8":31,"./utils":32,"./zipEntries":33}],12:[function(R,M,S){"use strict";var h=R("../utils"),f=R("../stream/GenericWorker");function r(c,w){f.call(this,"Nodejs stream input adapter for "+c),this._upstreamEnded=!1,this._bindStream(w)}h.inherits(r,f),r.prototype._bindStream=function(c){var w=this;(this._stream=c).pause(),c.on("data",(function(z){w.push({data:z,meta:{percent:0}})})).on("error",(function(z){w.isPaused?this.generatedError=z:w.error(z)})).on("end",(function(){w.isPaused?w._upstreamEnded=!0:w.end()}))},r.prototype.pause=function(){return!!f.prototype.pause.call(this)&&(this._stream.pause(),!0)},r.prototype.resume=function(){return!!f.prototype.resume.call(this)&&(this._upstreamEnded?this.end():this._stream.resume(),!0)},M.exports=r},{"../stream/GenericWorker":28,"../utils":32}],13:[function(R,M,S){"use strict";var h=R("readable-stream").Readable;function f(r,c,w){h.call(this,c),this._helper=r;var z=this;r.on("data",(function(k,_){z.push(k)||z._helper.pause(),w&&w(_)})).on("error",(function(k){z.emit("error",k)})).on("end",(function(){z.push(null)}))}R("../utils").inherits(f,h),f.prototype._read=function(){this._helper.resume()},M.exports=f},{"../utils":32,"readable-stream":16}],14:[function(R,M,S){"use strict";M.exports={isNode:typeof Buffer!="undefined",newBufferFrom:function(h,f){if(Buffer.from&&Buffer.from!==Uint8Array.from)return Buffer.from(h,f);if(typeof h=="number")throw new Error('The "data" argument must not be a number');return new Buffer(h,f)},allocBuffer:function(h){if(Buffer.alloc)return Buffer.alloc(h);var f=new Buffer(h);return f.fill(0),f},isBuffer:function(h){return Buffer.isBuffer(h)},isStream:function(h){return h&&typeof h.on=="function"&&typeof h.pause=="function"&&typeof h.resume=="function"}}},{}],15:[function(R,M,S){"use strict";function h(C,$,T){var H,P=r.getTypeOf($),Y=r.extend(T||{},z);Y.date=Y.date||new Date,Y.compression!==null&&(Y.compression=Y.compression.toUpperCase()),typeof Y.unixPermissions=="string"&&(Y.unixPermissions=parseInt(Y.unixPermissions,8)),Y.unixPermissions&&16384&Y.unixPermissions&&(Y.dir=!0),Y.dosPermissions&&16&Y.dosPermissions&&(Y.dir=!0),Y.dir&&(C=d(C)),Y.createFolders&&(H=m(C))&&x.call(this,H,!0);var ce=P==="string"&&Y.binary===!1&&Y.base64===!1;T&&T.binary!==void 0||(Y.binary=!ce),($ instanceof k&&$.uncompressedSize===0||Y.dir||!$||$.length===0)&&(Y.base64=!1,Y.binary=!0,$="",Y.compression="STORE",P="string");var y=null;y=$ instanceof k||$ instanceof c?$:v.isNode&&v.isStream($)?new i(C,$):r.prepareContent(C,$,Y.binary,Y.optimizedBinaryString,Y.base64);var B=new _(C,y,Y);this.files[C]=B}var f=R("./utf8"),r=R("./utils"),c=R("./stream/GenericWorker"),w=R("./stream/StreamHelper"),z=R("./defaults"),k=R("./compressedObject"),_=R("./zipObject"),o=R("./generate"),v=R("./nodejsUtils"),i=R("./nodejs/NodejsStreamInputAdapter"),m=function(C){C.slice(-1)==="/"&&(C=C.substring(0,C.length-1));var $=C.lastIndexOf("/");return 0<$?C.substring(0,$):""},d=function(C){return C.slice(-1)!=="/"&&(C+="/"),C},x=function(C,$){return $=$!==void 0?$:z.createFolders,C=d(C),this.files[C]||h.call(this,C,null,{dir:!0,createFolders:$}),this.files[C]};function E(C){return Object.prototype.toString.call(C)==="[object RegExp]"}var A={load:function(){throw new Error("This method has been removed in JSZip 3.0, please check the upgrade guide.")},forEach:function(C){var $,T,H;for($ in this.files)H=this.files[$],(T=$.slice(this.root.length,$.length))&&$.slice(0,this.root.length)===this.root&&C(T,H)},filter:function(C){var $=[];return this.forEach((function(T,H){C(T,H)&&$.push(H)})),$},file:function(C,$,T){if(arguments.length!==1)return C=this.root+C,h.call(this,C,$,T),this;if(E(C)){var H=C;return this.filter((function(Y,ce){return!ce.dir&&H.test(Y)}))}var P=this.files[this.root+C];return P&&!P.dir?P:null},folder:function(C){if(!C)return this;if(E(C))return this.filter((function(P,Y){return Y.dir&&C.test(P)}));var $=this.root+C,T=x.call(this,$),H=this.clone();return H.root=T.name,H},remove:function(C){C=this.root+C;var $=this.files[C];if($||(C.slice(-1)!=="/"&&(C+="/"),$=this.files[C]),$&&!$.dir)delete this.files[C];else for(var T=this.filter((function(P,Y){return Y.name.slice(0,C.length)===C})),H=0;H<T.length;H++)delete this.files[T[H].name];return this},generate:function(){throw new Error("This method has been removed in JSZip 3.0, please check the upgrade guide.")},generateInternalStream:function(C){var $,T={};try{if((T=r.extend(C||{},{streamFiles:!1,compression:"STORE",compressionOptions:null,type:"",platform:"DOS",comment:null,mimeType:"application/zip",encodeFileName:f.utf8encode})).type=T.type.toLowerCase(),T.compression=T.compression.toUpperCase(),T.type==="binarystring"&&(T.type="string"),!T.type)throw new Error("No output type specified.");r.checkSupport(T.type),T.platform!=="darwin"&&T.platform!=="freebsd"&&T.platform!=="linux"&&T.platform!=="sunos"||(T.platform="UNIX"),T.platform==="win32"&&(T.platform="DOS");var H=T.comment||this.comment||"";$=o.generateWorker(this,T,H)}catch(P){($=new c("error")).error(P)}return new w($,T.type||"string",T.mimeType)},generateAsync:function(C,$){return this.generateInternalStream(C).accumulate($)},generateNodeStream:function(C,$){return(C=C||{}).type||(C.type="nodebuffer"),this.generateInternalStream(C).toNodejsStream($)}};M.exports=A},{"./compressedObject":2,"./defaults":5,"./generate":9,"./nodejs/NodejsStreamInputAdapter":12,"./nodejsUtils":14,"./stream/GenericWorker":28,"./stream/StreamHelper":29,"./utf8":31,"./utils":32,"./zipObject":35}],16:[function(R,M,S){"use strict";M.exports=R("stream")},{stream:void 0}],17:[function(R,M,S){"use strict";var h=R("./DataReader");function f(r){h.call(this,r);for(var c=0;c<this.data.length;c++)r[c]=255&r[c]}R("../utils").inherits(f,h),f.prototype.byteAt=function(r){return this.data[this.zero+r]},f.prototype.lastIndexOfSignature=function(r){for(var c=r.charCodeAt(0),w=r.charCodeAt(1),z=r.charCodeAt(2),k=r.charCodeAt(3),_=this.length-4;0<=_;--_)if(this.data[_]===c&&this.data[_+1]===w&&this.data[_+2]===z&&this.data[_+3]===k)return _-this.zero;return-1},f.prototype.readAndCheckSignature=function(r){var c=r.charCodeAt(0),w=r.charCodeAt(1),z=r.charCodeAt(2),k=r.charCodeAt(3),_=this.readData(4);return c===_[0]&&w===_[1]&&z===_[2]&&k===_[3]},f.prototype.readData=function(r){if(this.checkOffset(r),r===0)return[];var c=this.data.slice(this.zero+this.index,this.zero+this.index+r);return this.index+=r,c},M.exports=f},{"../utils":32,"./DataReader":18}],18:[function(R,M,S){"use strict";var h=R("../utils");function f(r){this.data=r,this.length=r.length,this.index=0,this.zero=0}f.prototype={checkOffset:function(r){this.checkIndex(this.index+r)},checkIndex:function(r){if(this.length<this.zero+r||r<0)throw new Error("End of data reached (data length = "+this.length+", asked index = "+r+"). Corrupted zip ?")},setIndex:function(r){this.checkIndex(r),this.index=r},skip:function(r){this.setIndex(this.index+r)},byteAt:function(){},readInt:function(r){var c,w=0;for(this.checkOffset(r),c=this.index+r-1;c>=this.index;c--)w=(w<<8)+this.byteAt(c);return this.index+=r,w},readString:function(r){return h.transformTo("string",this.readData(r))},readData:function(){},lastIndexOfSignature:function(){},readAndCheckSignature:function(){},readDate:function(){var r=this.readInt(4);return new Date(Date.UTC(1980+(r>>25&127),(r>>21&15)-1,r>>16&31,r>>11&31,r>>5&63,(31&r)<<1))}},M.exports=f},{"../utils":32}],19:[function(R,M,S){"use strict";var h=R("./Uint8ArrayReader");function f(r){h.call(this,r)}R("../utils").inherits(f,h),f.prototype.readData=function(r){this.checkOffset(r);var c=this.data.slice(this.zero+this.index,this.zero+this.index+r);return this.index+=r,c},M.exports=f},{"../utils":32,"./Uint8ArrayReader":21}],20:[function(R,M,S){"use strict";var h=R("./DataReader");function f(r){h.call(this,r)}R("../utils").inherits(f,h),f.prototype.byteAt=function(r){return this.data.charCodeAt(this.zero+r)},f.prototype.lastIndexOfSignature=function(r){return this.data.lastIndexOf(r)-this.zero},f.prototype.readAndCheckSignature=function(r){return r===this.readData(4)},f.prototype.readData=function(r){this.checkOffset(r);var c=this.data.slice(this.zero+this.index,this.zero+this.index+r);return this.index+=r,c},M.exports=f},{"../utils":32,"./DataReader":18}],21:[function(R,M,S){"use strict";var h=R("./ArrayReader");function f(r){h.call(this,r)}R("../utils").inherits(f,h),f.prototype.readData=function(r){if(this.checkOffset(r),r===0)return new Uint8Array(0);var c=this.data.subarray(this.zero+this.index,this.zero+this.index+r);return this.index+=r,c},M.exports=f},{"../utils":32,"./ArrayReader":17}],22:[function(R,M,S){"use strict";var h=R("../utils"),f=R("../support"),r=R("./ArrayReader"),c=R("./StringReader"),w=R("./NodeBufferReader"),z=R("./Uint8ArrayReader");M.exports=function(k){var _=h.getTypeOf(k);return h.checkSupport(_),_!=="string"||f.uint8array?_==="nodebuffer"?new w(k):f.uint8array?new z(h.transformTo("uint8array",k)):new r(h.transformTo("array",k)):new c(k)}},{"../support":30,"../utils":32,"./ArrayReader":17,"./NodeBufferReader":19,"./StringReader":20,"./Uint8ArrayReader":21}],23:[function(R,M,S){"use strict";S.LOCAL_FILE_HEADER="PK",S.CENTRAL_FILE_HEADER="PK",S.CENTRAL_DIRECTORY_END="PK",S.ZIP64_CENTRAL_DIRECTORY_LOCATOR="PK\x07",S.ZIP64_CENTRAL_DIRECTORY_END="PK",S.DATA_DESCRIPTOR="PK\x07\b"},{}],24:[function(R,M,S){"use strict";var h=R("./GenericWorker"),f=R("../utils");function r(c){h.call(this,"ConvertWorker to "+c),this.destType=c}f.inherits(r,h),r.prototype.processChunk=function(c){this.push({data:f.transformTo(this.destType,c.data),meta:c.meta})},M.exports=r},{"../utils":32,"./GenericWorker":28}],25:[function(R,M,S){"use strict";var h=R("./GenericWorker"),f=R("../crc32");function r(){h.call(this,"Crc32Probe"),this.withStreamInfo("crc32",0)}R("../utils").inherits(r,h),r.prototype.processChunk=function(c){this.streamInfo.crc32=f(c.data,this.streamInfo.crc32||0),this.push(c)},M.exports=r},{"../crc32":4,"../utils":32,"./GenericWorker":28}],26:[function(R,M,S){"use strict";var h=R("../utils"),f=R("./GenericWorker");function r(c){f.call(this,"DataLengthProbe for "+c),this.propName=c,this.withStreamInfo(c,0)}h.inherits(r,f),r.prototype.processChunk=function(c){if(c){var w=this.streamInfo[this.propName]||0;this.streamInfo[this.propName]=w+c.data.length}f.prototype.processChunk.call(this,c)},M.exports=r},{"../utils":32,"./GenericWorker":28}],27:[function(R,M,S){"use strict";var h=R("../utils"),f=R("./GenericWorker");function r(c){f.call(this,"DataWorker");var w=this;this.dataIsReady=!1,this.index=0,this.max=0,this.data=null,this.type="",this._tickScheduled=!1,c.then((function(z){w.dataIsReady=!0,w.data=z,w.max=z&&z.length||0,w.type=h.getTypeOf(z),w.isPaused||w._tickAndRepeat()}),(function(z){w.error(z)}))}h.inherits(r,f),r.prototype.cleanUp=function(){f.prototype.cleanUp.call(this),this.data=null},r.prototype.resume=function(){return!!f.prototype.resume.call(this)&&(!this._tickScheduled&&this.dataIsReady&&(this._tickScheduled=!0,h.delay(this._tickAndRepeat,[],this)),!0)},r.prototype._tickAndRepeat=function(){this._tickScheduled=!1,this.isPaused||this.isFinished||(this._tick(),this.isFinished||(h.delay(this._tickAndRepeat,[],this),this._tickScheduled=!0))},r.prototype._tick=function(){if(this.isPaused||this.isFinished)return!1;var c=null,w=Math.min(this.max,this.index+16384);if(this.index>=this.max)return this.end();switch(this.type){case"string":c=this.data.substring(this.index,w);break;case"uint8array":c=this.data.subarray(this.index,w);break;case"array":case"nodebuffer":c=this.data.slice(this.index,w)}return this.index=w,this.push({data:c,meta:{percent:this.max?this.index/this.max*100:0}})},M.exports=r},{"../utils":32,"./GenericWorker":28}],28:[function(R,M,S){"use strict";function h(f){this.name=f||"default",this.streamInfo={},this.generatedError=null,this.extraStreamInfo={},this.isPaused=!0,this.isFinished=!1,this.isLocked=!1,this._listeners={data:[],end:[],error:[]},this.previous=null}h.prototype={push:function(f){this.emit("data",f)},end:function(){if(this.isFinished)return!1;this.flush();try{this.emit("end"),this.cleanUp(),this.isFinished=!0}catch(f){this.emit("error",f)}return!0},error:function(f){return!this.isFinished&&(this.isPaused?this.generatedError=f:(this.isFinished=!0,this.emit("error",f),this.previous&&this.previous.error(f),this.cleanUp()),!0)},on:function(f,r){return this._listeners[f].push(r),this},cleanUp:function(){this.streamInfo=this.generatedError=this.extraStreamInfo=null,this._listeners=[]},emit:function(f,r){if(this._listeners[f])for(var c=0;c<this._listeners[f].length;c++)this._listeners[f][c].call(this,r)},pipe:function(f){return f.registerPrevious(this)},registerPrevious:function(f){if(this.isLocked)throw new Error("The stream '"+this+"' has already been used.");this.streamInfo=f.streamInfo,this.mergeStreamInfo(),this.previous=f;var r=this;return f.on("data",(function(c){r.processChunk(c)})),f.on("end",(function(){r.end()})),f.on("error",(function(c){r.error(c)})),this},pause:function(){return!this.isPaused&&!this.isFinished&&(this.isPaused=!0,this.previous&&this.previous.pause(),!0)},resume:function(){if(!this.isPaused||this.isFinished)return!1;var f=this.isPaused=!1;return this.generatedError&&(this.error(this.generatedError),f=!0),this.previous&&this.previous.resume(),!f},flush:function(){},processChunk:function(f){this.push(f)},withStreamInfo:function(f,r){return this.extraStreamInfo[f]=r,this.mergeStreamInfo(),this},mergeStreamInfo:function(){for(var f in this.extraStreamInfo)Object.prototype.hasOwnProperty.call(this.extraStreamInfo,f)&&(this.streamInfo[f]=this.extraStreamInfo[f])},lock:function(){if(this.isLocked)throw new Error("The stream '"+this+"' has already been used.");this.isLocked=!0,this.previous&&this.previous.lock()},toString:function(){var f="Worker "+this.name;return this.previous?this.previous+" -> "+f:f}},M.exports=h},{}],29:[function(R,M,S){"use strict";var h=R("../utils"),f=R("./ConvertWorker"),r=R("./GenericWorker"),c=R("../base64"),w=R("../support"),z=R("../external"),k=null;if(w.nodestream)try{k=R("../nodejs/NodejsStreamOutputAdapter")}catch(v){}function _(v,i){return new z.Promise((function(m,d){var x=[],E=v._internalType,A=v._outputType,C=v._mimeType;v.on("data",(function($,T){x.push($),i&&i(T)})).on("error",(function($){x=[],d($)})).on("end",(function(){try{var $=(function(T,H,P){switch(T){case"blob":return h.newBlob(h.transformTo("arraybuffer",H),P);case"base64":return c.encode(H);default:return h.transformTo(T,H)}})(A,(function(T,H){var P,Y=0,ce=null,y=0;for(P=0;P<H.length;P++)y+=H[P].length;switch(T){case"string":return H.join("");case"array":return Array.prototype.concat.apply([],H);case"uint8array":for(ce=new Uint8Array(y),P=0;P<H.length;P++)ce.set(H[P],Y),Y+=H[P].length;return ce;case"nodebuffer":return Buffer.concat(H);default:throw new Error("concat : unsupported type '"+T+"'")}})(E,x),C);m($)}catch(T){d(T)}x=[]})).resume()}))}function o(v,i,m){var d=i;switch(i){case"blob":case"arraybuffer":d="uint8array";break;case"base64":d="string"}try{this._internalType=d,this._outputType=i,this._mimeType=m,h.checkSupport(d),this._worker=v.pipe(new f(d)),v.lock()}catch(x){this._worker=new r("error"),this._worker.error(x)}}o.prototype={accumulate:function(v){return _(this,v)},on:function(v,i){var m=this;return v==="data"?this._worker.on(v,(function(d){i.call(m,d.data,d.meta)})):this._worker.on(v,(function(){h.delay(i,arguments,m)})),this},resume:function(){return h.delay(this._worker.resume,[],this._worker),this},pause:function(){return this._worker.pause(),this},toNodejsStream:function(v){if(h.checkSupport("nodestream"),this._outputType!=="nodebuffer")throw new Error(this._outputType+" is not supported by this method");return new k(this,{objectMode:this._outputType!=="nodebuffer"},v)}},M.exports=o},{"../base64":1,"../external":6,"../nodejs/NodejsStreamOutputAdapter":13,"../support":30,"../utils":32,"./ConvertWorker":24,"./GenericWorker":28}],30:[function(R,M,S){"use strict";if(S.base64=!0,S.array=!0,S.string=!0,S.arraybuffer=typeof ArrayBuffer!="undefined"&&typeof Uint8Array!="undefined",S.nodebuffer=typeof Buffer!="undefined",S.uint8array=typeof Uint8Array!="undefined",typeof ArrayBuffer=="undefined")S.blob=!1;else{var h=new ArrayBuffer(0);try{S.blob=new Blob([h],{type:"application/zip"}).size===0}catch(r){try{var f=new(self.BlobBuilder||self.WebKitBlobBuilder||self.MozBlobBuilder||self.MSBlobBuilder);f.append(h),S.blob=f.getBlob("application/zip").size===0}catch(c){S.blob=!1}}}try{S.nodestream=!!R("readable-stream").Readable}catch(r){S.nodestream=!1}},{"readable-stream":16}],31:[function(R,M,S){"use strict";for(var h=R("./utils"),f=R("./support"),r=R("./nodejsUtils"),c=R("./stream/GenericWorker"),w=new Array(256),z=0;z<256;z++)w[z]=252<=z?6:248<=z?5:240<=z?4:224<=z?3:192<=z?2:1;function k(){c.call(this,"utf-8 decode"),this.leftOver=null}function _(){c.call(this,"utf-8 encode")}w[254]=w[254]=1,S.utf8encode=function(o){return f.nodebuffer?r.newBufferFrom(o,"utf-8"):(function(v){var i,m,d,x,E,A=v.length,C=0;for(x=0;x<A;x++)(64512&(m=v.charCodeAt(x)))==55296&&x+1<A&&(64512&(d=v.charCodeAt(x+1)))==56320&&(m=65536+(m-55296<<10)+(d-56320),x++),C+=m<128?1:m<2048?2:m<65536?3:4;for(i=f.uint8array?new Uint8Array(C):new Array(C),x=E=0;E<C;x++)(64512&(m=v.charCodeAt(x)))==55296&&x+1<A&&(64512&(d=v.charCodeAt(x+1)))==56320&&(m=65536+(m-55296<<10)+(d-56320),x++),m<128?i[E++]=m:(m<2048?i[E++]=192|m>>>6:(m<65536?i[E++]=224|m>>>12:(i[E++]=240|m>>>18,i[E++]=128|m>>>12&63),i[E++]=128|m>>>6&63),i[E++]=128|63&m);return i})(o)},S.utf8decode=function(o){return f.nodebuffer?h.transformTo("nodebuffer",o).toString("utf-8"):(function(v){var i,m,d,x,E=v.length,A=new Array(2*E);for(i=m=0;i<E;)if((d=v[i++])<128)A[m++]=d;else if(4<(x=w[d]))A[m++]=65533,i+=x-1;else{for(d&=x===2?31:x===3?15:7;1<x&&i<E;)d=d<<6|63&v[i++],x--;1<x?A[m++]=65533:d<65536?A[m++]=d:(d-=65536,A[m++]=55296|d>>10&1023,A[m++]=56320|1023&d)}return A.length!==m&&(A.subarray?A=A.subarray(0,m):A.length=m),h.applyFromCharCode(A)})(o=h.transformTo(f.uint8array?"uint8array":"array",o))},h.inherits(k,c),k.prototype.processChunk=function(o){var v=h.transformTo(f.uint8array?"uint8array":"array",o.data);if(this.leftOver&&this.leftOver.length){if(f.uint8array){var i=v;(v=new Uint8Array(i.length+this.leftOver.length)).set(this.leftOver,0),v.set(i,this.leftOver.length)}else v=this.leftOver.concat(v);this.leftOver=null}var m=(function(x,E){var A;for((E=E||x.length)>x.length&&(E=x.length),A=E-1;0<=A&&(192&x[A])==128;)A--;return A<0||A===0?E:A+w[x[A]]>E?A:E})(v),d=v;m!==v.length&&(f.uint8array?(d=v.subarray(0,m),this.leftOver=v.subarray(m,v.length)):(d=v.slice(0,m),this.leftOver=v.slice(m,v.length))),this.push({data:S.utf8decode(d),meta:o.meta})},k.prototype.flush=function(){this.leftOver&&this.leftOver.length&&(this.push({data:S.utf8decode(this.leftOver),meta:{}}),this.leftOver=null)},S.Utf8DecodeWorker=k,h.inherits(_,c),_.prototype.processChunk=function(o){this.push({data:S.utf8encode(o.data),meta:o.meta})},S.Utf8EncodeWorker=_},{"./nodejsUtils":14,"./stream/GenericWorker":28,"./support":30,"./utils":32}],32:[function(R,M,S){"use strict";var h=R("./support"),f=R("./base64"),r=R("./nodejsUtils"),c=R("./external");function w(i){return i}function z(i,m){for(var d=0;d<i.length;++d)m[d]=255&i.charCodeAt(d);return m}R("setimmediate"),S.newBlob=function(i,m){S.checkSupport("blob");try{return new Blob([i],{type:m})}catch(x){try{var d=new(self.BlobBuilder||self.WebKitBlobBuilder||self.MozBlobBuilder||self.MSBlobBuilder);return d.append(i),d.getBlob(m)}catch(E){throw new Error("Bug : can't construct the Blob.")}}};var k={stringifyByChunk:function(i,m,d){var x=[],E=0,A=i.length;if(A<=d)return String.fromCharCode.apply(null,i);for(;E<A;)m==="array"||m==="nodebuffer"?x.push(String.fromCharCode.apply(null,i.slice(E,Math.min(E+d,A)))):x.push(String.fromCharCode.apply(null,i.subarray(E,Math.min(E+d,A)))),E+=d;return x.join("")},stringifyByChar:function(i){for(var m="",d=0;d<i.length;d++)m+=String.fromCharCode(i[d]);return m},applyCanBeUsed:{uint8array:(function(){try{return h.uint8array&&String.fromCharCode.apply(null,new Uint8Array(1)).length===1}catch(i){return!1}})(),nodebuffer:(function(){try{return h.nodebuffer&&String.fromCharCode.apply(null,r.allocBuffer(1)).length===1}catch(i){return!1}})()}};function _(i){var m=65536,d=S.getTypeOf(i),x=!0;if(d==="uint8array"?x=k.applyCanBeUsed.uint8array:d==="nodebuffer"&&(x=k.applyCanBeUsed.nodebuffer),x)for(;1<m;)try{return k.stringifyByChunk(i,d,m)}catch(E){m=Math.floor(m/2)}return k.stringifyByChar(i)}function o(i,m){for(var d=0;d<i.length;d++)m[d]=i[d];return m}S.applyFromCharCode=_;var v={};v.string={string:w,array:function(i){return z(i,new Array(i.length))},arraybuffer:function(i){return v.string.uint8array(i).buffer},uint8array:function(i){return z(i,new Uint8Array(i.length))},nodebuffer:function(i){return z(i,r.allocBuffer(i.length))}},v.array={string:_,array:w,arraybuffer:function(i){return new Uint8Array(i).buffer},uint8array:function(i){return new Uint8Array(i)},nodebuffer:function(i){return r.newBufferFrom(i)}},v.arraybuffer={string:function(i){return _(new Uint8Array(i))},array:function(i){return o(new Uint8Array(i),new Array(i.byteLength))},arraybuffer:w,uint8array:function(i){return new Uint8Array(i)},nodebuffer:function(i){return r.newBufferFrom(new Uint8Array(i))}},v.uint8array={string:_,array:function(i){return o(i,new Array(i.length))},arraybuffer:function(i){return i.buffer},uint8array:w,nodebuffer:function(i){return r.newBufferFrom(i)}},v.nodebuffer={string:_,array:function(i){return o(i,new Array(i.length))},arraybuffer:function(i){return v.nodebuffer.uint8array(i).buffer},uint8array:function(i){return o(i,new Uint8Array(i.length))},nodebuffer:w},S.transformTo=function(i,m){if(m=m||"",!i)return m;S.checkSupport(i);var d=S.getTypeOf(m);return v[d][i](m)},S.resolve=function(i){for(var m=i.split("/"),d=[],x=0;x<m.length;x++){var E=m[x];E==="."||E===""&&x!==0&&x!==m.length-1||(E===".."?d.pop():d.push(E))}return d.join("/")},S.getTypeOf=function(i){return typeof i=="string"?"string":Object.prototype.toString.call(i)==="[object Array]"?"array":h.nodebuffer&&r.isBuffer(i)?"nodebuffer":h.uint8array&&i instanceof Uint8Array?"uint8array":h.arraybuffer&&i instanceof ArrayBuffer?"arraybuffer":void 0},S.checkSupport=function(i){if(!h[i.toLowerCase()])throw new Error(i+" is not supported by this platform")},S.MAX_VALUE_16BITS=65535,S.MAX_VALUE_32BITS=-1,S.pretty=function(i){var m,d,x="";for(d=0;d<(i||"").length;d++)x+="\\x"+((m=i.charCodeAt(d))<16?"0":"")+m.toString(16).toUpperCase();return x},S.delay=function(i,m,d){setImmediate((function(){i.apply(d||null,m||[])}))},S.inherits=function(i,m){function d(){}d.prototype=m.prototype,i.prototype=new d},S.extend=function(){var i,m,d={};for(i=0;i<arguments.length;i++)for(m in arguments[i])Object.prototype.hasOwnProperty.call(arguments[i],m)&&d[m]===void 0&&(d[m]=arguments[i][m]);return d},S.prepareContent=function(i,m,d,x,E){return c.Promise.resolve(m).then((function(A){return h.blob&&(A instanceof Blob||["[object File]","[object Blob]"].indexOf(Object.prototype.toString.call(A))!==-1)&&typeof FileReader!="undefined"?new c.Promise((function(C,$){var T=new FileReader;T.onload=function(H){C(H.target.result)},T.onerror=function(H){$(H.target.error)},T.readAsArrayBuffer(A)})):A})).then((function(A){var C=S.getTypeOf(A);return C?(C==="arraybuffer"?A=S.transformTo("uint8array",A):C==="string"&&(E?A=f.decode(A):d&&x!==!0&&(A=(function($){return z($,h.uint8array?new Uint8Array($.length):new Array($.length))})(A))),A):c.Promise.reject(new Error("Can't read the data of '"+i+"'. Is it in a supported JavaScript type (String, Blob, ArrayBuffer, etc) ?"))}))}},{"./base64":1,"./external":6,"./nodejsUtils":14,"./support":30,setimmediate:54}],33:[function(R,M,S){"use strict";var h=R("./reader/readerFor"),f=R("./utils"),r=R("./signature"),c=R("./zipEntry"),w=R("./support");function z(k){this.files=[],this.loadOptions=k}z.prototype={checkSignature:function(k){if(!this.reader.readAndCheckSignature(k)){this.reader.index-=4;var _=this.reader.readString(4);throw new Error("Corrupted zip or bug: unexpected signature ("+f.pretty(_)+", expected "+f.pretty(k)+")")}},isSignature:function(k,_){var o=this.reader.index;this.reader.setIndex(k);var v=this.reader.readString(4)===_;return this.reader.setIndex(o),v},readBlockEndOfCentral:function(){this.diskNumber=this.reader.readInt(2),this.diskWithCentralDirStart=this.reader.readInt(2),this.centralDirRecordsOnThisDisk=this.reader.readInt(2),this.centralDirRecords=this.reader.readInt(2),this.centralDirSize=this.reader.readInt(4),this.centralDirOffset=this.reader.readInt(4),this.zipCommentLength=this.reader.readInt(2);var k=this.reader.readData(this.zipCommentLength),_=w.uint8array?"uint8array":"array",o=f.transformTo(_,k);this.zipComment=this.loadOptions.decodeFileName(o)},readBlockZip64EndOfCentral:function(){this.zip64EndOfCentralSize=this.reader.readInt(8),this.reader.skip(4),this.diskNumber=this.reader.readInt(4),this.diskWithCentralDirStart=this.reader.readInt(4),this.centralDirRecordsOnThisDisk=this.reader.readInt(8),this.centralDirRecords=this.reader.readInt(8),this.centralDirSize=this.reader.readInt(8),this.centralDirOffset=this.reader.readInt(8),this.zip64ExtensibleData={};for(var k,_,o,v=this.zip64EndOfCentralSize-44;0<v;)k=this.reader.readInt(2),_=this.reader.readInt(4),o=this.reader.readData(_),this.zip64ExtensibleData[k]={id:k,length:_,value:o}},readBlockZip64EndOfCentralLocator:function(){if(this.diskWithZip64CentralDirStart=this.reader.readInt(4),this.relativeOffsetEndOfZip64CentralDir=this.reader.readInt(8),this.disksCount=this.reader.readInt(4),1<this.disksCount)throw new Error("Multi-volumes zip are not supported")},readLocalFiles:function(){var k,_;for(k=0;k<this.files.length;k++)_=this.files[k],this.reader.setIndex(_.localHeaderOffset),this.checkSignature(r.LOCAL_FILE_HEADER),_.readLocalPart(this.reader),_.handleUTF8(),_.processAttributes()},readCentralDir:function(){var k;for(this.reader.setIndex(this.centralDirOffset);this.reader.readAndCheckSignature(r.CENTRAL_FILE_HEADER);)(k=new c({zip64:this.zip64},this.loadOptions)).readCentralPart(this.reader),this.files.push(k);if(this.centralDirRecords!==this.files.length&&this.centralDirRecords!==0&&this.files.length===0)throw new Error("Corrupted zip or bug: expected "+this.centralDirRecords+" records in central dir, got "+this.files.length)},readEndOfCentral:function(){var k=this.reader.lastIndexOfSignature(r.CENTRAL_DIRECTORY_END);if(k<0)throw this.isSignature(0,r.LOCAL_FILE_HEADER)?new Error("Corrupted zip: can't find end of central directory"):new Error("Can't find end of central directory : is this a zip file ? If it is, see https://stuk.github.io/jszip/documentation/howto/read_zip.html");this.reader.setIndex(k);var _=k;if(this.checkSignature(r.CENTRAL_DIRECTORY_END),this.readBlockEndOfCentral(),this.diskNumber===f.MAX_VALUE_16BITS||this.diskWithCentralDirStart===f.MAX_VALUE_16BITS||this.centralDirRecordsOnThisDisk===f.MAX_VALUE_16BITS||this.centralDirRecords===f.MAX_VALUE_16BITS||this.centralDirSize===f.MAX_VALUE_32BITS||this.centralDirOffset===f.MAX_VALUE_32BITS){if(this.zip64=!0,(k=this.reader.lastIndexOfSignature(r.ZIP64_CENTRAL_DIRECTORY_LOCATOR))<0)throw new Error("Corrupted zip: can't find the ZIP64 end of central directory locator");if(this.reader.setIndex(k),this.checkSignature(r.ZIP64_CENTRAL_DIRECTORY_LOCATOR),this.readBlockZip64EndOfCentralLocator(),!this.isSignature(this.relativeOffsetEndOfZip64CentralDir,r.ZIP64_CENTRAL_DIRECTORY_END)&&(this.relativeOffsetEndOfZip64CentralDir=this.reader.lastIndexOfSignature(r.ZIP64_CENTRAL_DIRECTORY_END),this.relativeOffsetEndOfZip64CentralDir<0))throw new Error("Corrupted zip: can't find the ZIP64 end of central directory");this.reader.setIndex(this.relativeOffsetEndOfZip64CentralDir),this.checkSignature(r.ZIP64_CENTRAL_DIRECTORY_END),this.readBlockZip64EndOfCentral()}var o=this.centralDirOffset+this.centralDirSize;this.zip64&&(o+=20,o+=12+this.zip64EndOfCentralSize);var v=_-o;if(0<v)this.isSignature(_,r.CENTRAL_FILE_HEADER)||(this.reader.zero=v);else if(v<0)throw new Error("Corrupted zip: missing "+Math.abs(v)+" bytes.")},prepareReader:function(k){this.reader=h(k)},load:function(k){this.prepareReader(k),this.readEndOfCentral(),this.readCentralDir(),this.readLocalFiles()}},M.exports=z},{"./reader/readerFor":22,"./signature":23,"./support":30,"./utils":32,"./zipEntry":34}],34:[function(R,M,S){"use strict";var h=R("./reader/readerFor"),f=R("./utils"),r=R("./compressedObject"),c=R("./crc32"),w=R("./utf8"),z=R("./compressions"),k=R("./support");function _(o,v){this.options=o,this.loadOptions=v}_.prototype={isEncrypted:function(){return(1&this.bitFlag)==1},useUTF8:function(){return(2048&this.bitFlag)==2048},readLocalPart:function(o){var v,i;if(o.skip(22),this.fileNameLength=o.readInt(2),i=o.readInt(2),this.fileName=o.readData(this.fileNameLength),o.skip(i),this.compressedSize===-1||this.uncompressedSize===-1)throw new Error("Bug or corrupted zip : didn't get enough information from the central directory (compressedSize === -1 || uncompressedSize === -1)");if((v=(function(m){for(var d in z)if(Object.prototype.hasOwnProperty.call(z,d)&&z[d].magic===m)return z[d];return null})(this.compressionMethod))===null)throw new Error("Corrupted zip : compression "+f.pretty(this.compressionMethod)+" unknown (inner file : "+f.transformTo("string",this.fileName)+")");this.decompressed=new r(this.compressedSize,this.uncompressedSize,this.crc32,v,o.readData(this.compressedSize))},readCentralPart:function(o){this.versionMadeBy=o.readInt(2),o.skip(2),this.bitFlag=o.readInt(2),this.compressionMethod=o.readString(2),this.date=o.readDate(),this.crc32=o.readInt(4),this.compressedSize=o.readInt(4),this.uncompressedSize=o.readInt(4);var v=o.readInt(2);if(this.extraFieldsLength=o.readInt(2),this.fileCommentLength=o.readInt(2),this.diskNumberStart=o.readInt(2),this.internalFileAttributes=o.readInt(2),this.externalFileAttributes=o.readInt(4),this.localHeaderOffset=o.readInt(4),this.isEncrypted())throw new Error("Encrypted zip are not supported");o.skip(v),this.readExtraFields(o),this.parseZIP64ExtraField(o),this.fileComment=o.readData(this.fileCommentLength)},processAttributes:function(){this.unixPermissions=null,this.dosPermissions=null;var o=this.versionMadeBy>>8;this.dir=!!(16&this.externalFileAttributes),o==0&&(this.dosPermissions=63&this.externalFileAttributes),o==3&&(this.unixPermissions=this.externalFileAttributes>>16&65535),this.dir||this.fileNameStr.slice(-1)!=="/"||(this.dir=!0)},parseZIP64ExtraField:function(){if(this.extraFields[1]){var o=h(this.extraFields[1].value);this.uncompressedSize===f.MAX_VALUE_32BITS&&(this.uncompressedSize=o.readInt(8)),this.compressedSize===f.MAX_VALUE_32BITS&&(this.compressedSize=o.readInt(8)),this.localHeaderOffset===f.MAX_VALUE_32BITS&&(this.localHeaderOffset=o.readInt(8)),this.diskNumberStart===f.MAX_VALUE_32BITS&&(this.diskNumberStart=o.readInt(4))}},readExtraFields:function(o){var v,i,m,d=o.index+this.extraFieldsLength;for(this.extraFields||(this.extraFields={});o.index+4<d;)v=o.readInt(2),i=o.readInt(2),m=o.readData(i),this.extraFields[v]={id:v,length:i,value:m};o.setIndex(d)},handleUTF8:function(){var o=k.uint8array?"uint8array":"array";if(this.useUTF8())this.fileNameStr=w.utf8decode(this.fileName),this.fileCommentStr=w.utf8decode(this.fileComment);else{var v=this.findExtraFieldUnicodePath();if(v!==null)this.fileNameStr=v;else{var i=f.transformTo(o,this.fileName);this.fileNameStr=this.loadOptions.decodeFileName(i)}var m=this.findExtraFieldUnicodeComment();if(m!==null)this.fileCommentStr=m;else{var d=f.transformTo(o,this.fileComment);this.fileCommentStr=this.loadOptions.decodeFileName(d)}}},findExtraFieldUnicodePath:function(){var o=this.extraFields[28789];if(o){var v=h(o.value);return v.readInt(1)!==1||c(this.fileName)!==v.readInt(4)?null:w.utf8decode(v.readData(o.length-5))}return null},findExtraFieldUnicodeComment:function(){var o=this.extraFields[25461];if(o){var v=h(o.value);return v.readInt(1)!==1||c(this.fileComment)!==v.readInt(4)?null:w.utf8decode(v.readData(o.length-5))}return null}},M.exports=_},{"./compressedObject":2,"./compressions":3,"./crc32":4,"./reader/readerFor":22,"./support":30,"./utf8":31,"./utils":32}],35:[function(R,M,S){"use strict";function h(v,i,m){this.name=v,this.dir=m.dir,this.date=m.date,this.comment=m.comment,this.unixPermissions=m.unixPermissions,this.dosPermissions=m.dosPermissions,this._data=i,this._dataBinary=m.binary,this.options={compression:m.compression,compressionOptions:m.compressionOptions}}var f=R("./stream/StreamHelper"),r=R("./stream/DataWorker"),c=R("./utf8"),w=R("./compressedObject"),z=R("./stream/GenericWorker");h.prototype={internalStream:function(v){var i=null,m="string";try{if(!v)throw new Error("No output type specified.");var d=(m=v.toLowerCase())==="string"||m==="text";m!=="binarystring"&&m!=="text"||(m="string"),i=this._decompressWorker();var x=!this._dataBinary;x&&!d&&(i=i.pipe(new c.Utf8EncodeWorker)),!x&&d&&(i=i.pipe(new c.Utf8DecodeWorker))}catch(E){(i=new z("error")).error(E)}return new f(i,m,"")},async:function(v,i){return this.internalStream(v).accumulate(i)},nodeStream:function(v,i){return this.internalStream(v||"nodebuffer").toNodejsStream(i)},_compressWorker:function(v,i){if(this._data instanceof w&&this._data.compression.magic===v.magic)return this._data.getCompressedWorker();var m=this._decompressWorker();return this._dataBinary||(m=m.pipe(new c.Utf8EncodeWorker)),w.createWorkerFrom(m,v,i)},_decompressWorker:function(){return this._data instanceof w?this._data.getContentWorker():this._data instanceof z?this._data:new r(this._data)}};for(var k=["asText","asBinary","asNodeBuffer","asUint8Array","asArrayBuffer"],_=function(){throw new Error("This method has been removed in JSZip 3.0, please check the upgrade guide.")},o=0;o<k.length;o++)h.prototype[k[o]]=_;M.exports=h},{"./compressedObject":2,"./stream/DataWorker":27,"./stream/GenericWorker":28,"./stream/StreamHelper":29,"./utf8":31}],36:[function(R,M,S){(function(h){"use strict";var f,r,c=h.MutationObserver||h.WebKitMutationObserver;if(c){var w=0,z=new c(v),k=h.document.createTextNode("");z.observe(k,{characterData:!0}),f=function(){k.data=w=++w%2}}else if(h.setImmediate||h.MessageChannel===void 0)f="document"in h&&"onreadystatechange"in h.document.createElement("script")?function(){var i=h.document.createElement("script");i.onreadystatechange=function(){v(),i.onreadystatechange=null,i.parentNode.removeChild(i),i=null},h.document.documentElement.appendChild(i)}:function(){setTimeout(v,0)};else{var _=new h.MessageChannel;_.port1.onmessage=v,f=function(){_.port2.postMessage(0)}}var o=[];function v(){var i,m;r=!0;for(var d=o.length;d;){for(m=o,o=[],i=-1;++i<d;)m[i]();d=o.length}r=!1}M.exports=function(i){o.push(i)!==1||r||f()}}).call(this,ze.g!==void 0?ze.g:typeof self!="undefined"?self:typeof window!="undefined"?window:{})},{}],37:[function(R,M,S){"use strict";var h=R("immediate");function f(){}var r={},c=["REJECTED"],w=["FULFILLED"],z=["PENDING"];function k(d){if(typeof d!="function")throw new TypeError("resolver must be a function");this.state=z,this.queue=[],this.outcome=void 0,d!==f&&i(this,d)}function _(d,x,E){this.promise=d,typeof x=="function"&&(this.onFulfilled=x,this.callFulfilled=this.otherCallFulfilled),typeof E=="function"&&(this.onRejected=E,this.callRejected=this.otherCallRejected)}function o(d,x,E){h((function(){var A;try{A=x(E)}catch(C){return r.reject(d,C)}A===d?r.reject(d,new TypeError("Cannot resolve promise with itself")):r.resolve(d,A)}))}function v(d){var x=d&&d.then;if(d&&(typeof d=="object"||typeof d=="function")&&typeof x=="function")return function(){x.apply(d,arguments)}}function i(d,x){var E=!1;function A(T){E||(E=!0,r.reject(d,T))}function C(T){E||(E=!0,r.resolve(d,T))}var $=m((function(){x(C,A)}));$.status==="error"&&A($.value)}function m(d,x){var E={};try{E.value=d(x),E.status="success"}catch(A){E.status="error",E.value=A}return E}(M.exports=k).prototype.finally=function(d){if(typeof d!="function")return this;var x=this.constructor;return this.then((function(E){return x.resolve(d()).then((function(){return E}))}),(function(E){return x.resolve(d()).then((function(){throw E}))}))},k.prototype.catch=function(d){return this.then(null,d)},k.prototype.then=function(d,x){if(typeof d!="function"&&this.state===w||typeof x!="function"&&this.state===c)return this;var E=new this.constructor(f);return this.state!==z?o(E,this.state===w?d:x,this.outcome):this.queue.push(new _(E,d,x)),E},_.prototype.callFulfilled=function(d){r.resolve(this.promise,d)},_.prototype.otherCallFulfilled=function(d){o(this.promise,this.onFulfilled,d)},_.prototype.callRejected=function(d){r.reject(this.promise,d)},_.prototype.otherCallRejected=function(d){o(this.promise,this.onRejected,d)},r.resolve=function(d,x){var E=m(v,x);if(E.status==="error")return r.reject(d,E.value);var A=E.value;if(A)i(d,A);else{d.state=w,d.outcome=x;for(var C=-1,$=d.queue.length;++C<$;)d.queue[C].callFulfilled(x)}return d},r.reject=function(d,x){d.state=c,d.outcome=x;for(var E=-1,A=d.queue.length;++E<A;)d.queue[E].callRejected(x);return d},k.resolve=function(d){return d instanceof this?d:r.resolve(new this(f),d)},k.reject=function(d){var x=new this(f);return r.reject(x,d)},k.all=function(d){var x=this;if(Object.prototype.toString.call(d)!=="[object Array]")return this.reject(new TypeError("must be an array"));var E=d.length,A=!1;if(!E)return this.resolve([]);for(var C=new Array(E),$=0,T=-1,H=new this(f);++T<E;)P(d[T],T);return H;function P(Y,ce){x.resolve(Y).then((function(y){C[ce]=y,++$!==E||A||(A=!0,r.resolve(H,C))}),(function(y){A||(A=!0,r.reject(H,y))}))}},k.race=function(d){var x=this;if(Object.prototype.toString.call(d)!=="[object Array]")return this.reject(new TypeError("must be an array"));var E=d.length,A=!1;if(!E)return this.resolve([]);for(var C,$=-1,T=new this(f);++$<E;)C=d[$],x.resolve(C).then((function(H){A||(A=!0,r.resolve(T,H))}),(function(H){A||(A=!0,r.reject(T,H))}));return T}},{immediate:36}],38:[function(R,M,S){"use strict";var h={};(0,R("./lib/utils/common").assign)(h,R("./lib/deflate"),R("./lib/inflate"),R("./lib/zlib/constants")),M.exports=h},{"./lib/deflate":39,"./lib/inflate":40,"./lib/utils/common":41,"./lib/zlib/constants":44}],39:[function(R,M,S){"use strict";var h=R("./zlib/deflate"),f=R("./utils/common"),r=R("./utils/strings"),c=R("./zlib/messages"),w=R("./zlib/zstream"),z=Object.prototype.toString,k=0,_=-1,o=0,v=8;function i(d){if(!(this instanceof i))return new i(d);this.options=f.assign({level:_,method:v,chunkSize:16384,windowBits:15,memLevel:8,strategy:o,to:""},d||{});var x=this.options;x.raw&&0<x.windowBits?x.windowBits=-x.windowBits:x.gzip&&0<x.windowBits&&x.windowBits<16&&(x.windowBits+=16),this.err=0,this.msg="",this.ended=!1,this.chunks=[],this.strm=new w,this.strm.avail_out=0;var E=h.deflateInit2(this.strm,x.level,x.method,x.windowBits,x.memLevel,x.strategy);if(E!==k)throw new Error(c[E]);if(x.header&&h.deflateSetHeader(this.strm,x.header),x.dictionary){var A;if(A=typeof x.dictionary=="string"?r.string2buf(x.dictionary):z.call(x.dictionary)==="[object ArrayBuffer]"?new Uint8Array(x.dictionary):x.dictionary,(E=h.deflateSetDictionary(this.strm,A))!==k)throw new Error(c[E]);this._dict_set=!0}}function m(d,x){var E=new i(x);if(E.push(d,!0),E.err)throw E.msg||c[E.err];return E.result}i.prototype.push=function(d,x){var E,A,C=this.strm,$=this.options.chunkSize;if(this.ended)return!1;A=x===~~x?x:x===!0?4:0,typeof d=="string"?C.input=r.string2buf(d):z.call(d)==="[object ArrayBuffer]"?C.input=new Uint8Array(d):C.input=d,C.next_in=0,C.avail_in=C.input.length;do{if(C.avail_out===0&&(C.output=new f.Buf8($),C.next_out=0,C.avail_out=$),(E=h.deflate(C,A))!==1&&E!==k)return this.onEnd(E),!(this.ended=!0);C.avail_out!==0&&(C.avail_in!==0||A!==4&&A!==2)||(this.options.to==="string"?this.onData(r.buf2binstring(f.shrinkBuf(C.output,C.next_out))):this.onData(f.shrinkBuf(C.output,C.next_out)))}while((0<C.avail_in||C.avail_out===0)&&E!==1);return A===4?(E=h.deflateEnd(this.strm),this.onEnd(E),this.ended=!0,E===k):A!==2||(this.onEnd(k),!(C.avail_out=0))},i.prototype.onData=function(d){this.chunks.push(d)},i.prototype.onEnd=function(d){d===k&&(this.options.to==="string"?this.result=this.chunks.join(""):this.result=f.flattenChunks(this.chunks)),this.chunks=[],this.err=d,this.msg=this.strm.msg},S.Deflate=i,S.deflate=m,S.deflateRaw=function(d,x){return(x=x||{}).raw=!0,m(d,x)},S.gzip=function(d,x){return(x=x||{}).gzip=!0,m(d,x)}},{"./utils/common":41,"./utils/strings":42,"./zlib/deflate":46,"./zlib/messages":51,"./zlib/zstream":53}],40:[function(R,M,S){"use strict";var h=R("./zlib/inflate"),f=R("./utils/common"),r=R("./utils/strings"),c=R("./zlib/constants"),w=R("./zlib/messages"),z=R("./zlib/zstream"),k=R("./zlib/gzheader"),_=Object.prototype.toString;function o(i){if(!(this instanceof o))return new o(i);this.options=f.assign({chunkSize:16384,windowBits:0,to:""},i||{});var m=this.options;m.raw&&0<=m.windowBits&&m.windowBits<16&&(m.windowBits=-m.windowBits,m.windowBits===0&&(m.windowBits=-15)),!(0<=m.windowBits&&m.windowBits<16)||i&&i.windowBits||(m.windowBits+=32),15<m.windowBits&&m.windowBits<48&&(15&m.windowBits)==0&&(m.windowBits|=15),this.err=0,this.msg="",this.ended=!1,this.chunks=[],this.strm=new z,this.strm.avail_out=0;var d=h.inflateInit2(this.strm,m.windowBits);if(d!==c.Z_OK)throw new Error(w[d]);this.header=new k,h.inflateGetHeader(this.strm,this.header)}function v(i,m){var d=new o(m);if(d.push(i,!0),d.err)throw d.msg||w[d.err];return d.result}o.prototype.push=function(i,m){var d,x,E,A,C,$,T=this.strm,H=this.options.chunkSize,P=this.options.dictionary,Y=!1;if(this.ended)return!1;x=m===~~m?m:m===!0?c.Z_FINISH:c.Z_NO_FLUSH,typeof i=="string"?T.input=r.binstring2buf(i):_.call(i)==="[object ArrayBuffer]"?T.input=new Uint8Array(i):T.input=i,T.next_in=0,T.avail_in=T.input.length;do{if(T.avail_out===0&&(T.output=new f.Buf8(H),T.next_out=0,T.avail_out=H),(d=h.inflate(T,c.Z_NO_FLUSH))===c.Z_NEED_DICT&&P&&($=typeof P=="string"?r.string2buf(P):_.call(P)==="[object ArrayBuffer]"?new Uint8Array(P):P,d=h.inflateSetDictionary(this.strm,$)),d===c.Z_BUF_ERROR&&Y===!0&&(d=c.Z_OK,Y=!1),d!==c.Z_STREAM_END&&d!==c.Z_OK)return this.onEnd(d),!(this.ended=!0);T.next_out&&(T.avail_out!==0&&d!==c.Z_STREAM_END&&(T.avail_in!==0||x!==c.Z_FINISH&&x!==c.Z_SYNC_FLUSH)||(this.options.to==="string"?(E=r.utf8border(T.output,T.next_out),A=T.next_out-E,C=r.buf2string(T.output,E),T.next_out=A,T.avail_out=H-A,A&&f.arraySet(T.output,T.output,E,A,0),this.onData(C)):this.onData(f.shrinkBuf(T.output,T.next_out)))),T.avail_in===0&&T.avail_out===0&&(Y=!0)}while((0<T.avail_in||T.avail_out===0)&&d!==c.Z_STREAM_END);return d===c.Z_STREAM_END&&(x=c.Z_FINISH),x===c.Z_FINISH?(d=h.inflateEnd(this.strm),this.onEnd(d),this.ended=!0,d===c.Z_OK):x!==c.Z_SYNC_FLUSH||(this.onEnd(c.Z_OK),!(T.avail_out=0))},o.prototype.onData=function(i){this.chunks.push(i)},o.prototype.onEnd=function(i){i===c.Z_OK&&(this.options.to==="string"?this.result=this.chunks.join(""):this.result=f.flattenChunks(this.chunks)),this.chunks=[],this.err=i,this.msg=this.strm.msg},S.Inflate=o,S.inflate=v,S.inflateRaw=function(i,m){return(m=m||{}).raw=!0,v(i,m)},S.ungzip=v},{"./utils/common":41,"./utils/strings":42,"./zlib/constants":44,"./zlib/gzheader":47,"./zlib/inflate":49,"./zlib/messages":51,"./zlib/zstream":53}],41:[function(R,M,S){"use strict";var h=typeof Uint8Array!="undefined"&&typeof Uint16Array!="undefined"&&typeof Int32Array!="undefined";S.assign=function(c){for(var w=Array.prototype.slice.call(arguments,1);w.length;){var z=w.shift();if(z){if(typeof z!="object")throw new TypeError(z+"must be non-object");for(var k in z)z.hasOwnProperty(k)&&(c[k]=z[k])}}return c},S.shrinkBuf=function(c,w){return c.length===w?c:c.subarray?c.subarray(0,w):(c.length=w,c)};var f={arraySet:function(c,w,z,k,_){if(w.subarray&&c.subarray)c.set(w.subarray(z,z+k),_);else for(var o=0;o<k;o++)c[_+o]=w[z+o]},flattenChunks:function(c){var w,z,k,_,o,v;for(w=k=0,z=c.length;w<z;w++)k+=c[w].length;for(v=new Uint8Array(k),w=_=0,z=c.length;w<z;w++)o=c[w],v.set(o,_),_+=o.length;return v}},r={arraySet:function(c,w,z,k,_){for(var o=0;o<k;o++)c[_+o]=w[z+o]},flattenChunks:function(c){return[].concat.apply([],c)}};S.setTyped=function(c){c?(S.Buf8=Uint8Array,S.Buf16=Uint16Array,S.Buf32=Int32Array,S.assign(S,f)):(S.Buf8=Array,S.Buf16=Array,S.Buf32=Array,S.assign(S,r))},S.setTyped(h)},{}],42:[function(R,M,S){"use strict";var h=R("./common"),f=!0,r=!0;try{String.fromCharCode.apply(null,[0])}catch(k){f=!1}try{String.fromCharCode.apply(null,new Uint8Array(1))}catch(k){r=!1}for(var c=new h.Buf8(256),w=0;w<256;w++)c[w]=252<=w?6:248<=w?5:240<=w?4:224<=w?3:192<=w?2:1;function z(k,_){if(_<65537&&(k.subarray&&r||!k.subarray&&f))return String.fromCharCode.apply(null,h.shrinkBuf(k,_));for(var o="",v=0;v<_;v++)o+=String.fromCharCode(k[v]);return o}c[254]=c[254]=1,S.string2buf=function(k){var _,o,v,i,m,d=k.length,x=0;for(i=0;i<d;i++)(64512&(o=k.charCodeAt(i)))==55296&&i+1<d&&(64512&(v=k.charCodeAt(i+1)))==56320&&(o=65536+(o-55296<<10)+(v-56320),i++),x+=o<128?1:o<2048?2:o<65536?3:4;for(_=new h.Buf8(x),i=m=0;m<x;i++)(64512&(o=k.charCodeAt(i)))==55296&&i+1<d&&(64512&(v=k.charCodeAt(i+1)))==56320&&(o=65536+(o-55296<<10)+(v-56320),i++),o<128?_[m++]=o:(o<2048?_[m++]=192|o>>>6:(o<65536?_[m++]=224|o>>>12:(_[m++]=240|o>>>18,_[m++]=128|o>>>12&63),_[m++]=128|o>>>6&63),_[m++]=128|63&o);return _},S.buf2binstring=function(k){return z(k,k.length)},S.binstring2buf=function(k){for(var _=new h.Buf8(k.length),o=0,v=_.length;o<v;o++)_[o]=k.charCodeAt(o);return _},S.buf2string=function(k,_){var o,v,i,m,d=_||k.length,x=new Array(2*d);for(o=v=0;o<d;)if((i=k[o++])<128)x[v++]=i;else if(4<(m=c[i]))x[v++]=65533,o+=m-1;else{for(i&=m===2?31:m===3?15:7;1<m&&o<d;)i=i<<6|63&k[o++],m--;1<m?x[v++]=65533:i<65536?x[v++]=i:(i-=65536,x[v++]=55296|i>>10&1023,x[v++]=56320|1023&i)}return z(x,v)},S.utf8border=function(k,_){var o;for((_=_||k.length)>k.length&&(_=k.length),o=_-1;0<=o&&(192&k[o])==128;)o--;return o<0||o===0?_:o+c[k[o]]>_?o:_}},{"./common":41}],43:[function(R,M,S){"use strict";M.exports=function(h,f,r,c){for(var w=65535&h|0,z=h>>>16&65535|0,k=0;r!==0;){for(r-=k=2e3<r?2e3:r;z=z+(w=w+f[c++]|0)|0,--k;);w%=65521,z%=65521}return w|z<<16|0}},{}],44:[function(R,M,S){"use strict";M.exports={Z_NO_FLUSH:0,Z_PARTIAL_FLUSH:1,Z_SYNC_FLUSH:2,Z_FULL_FLUSH:3,Z_FINISH:4,Z_BLOCK:5,Z_TREES:6,Z_OK:0,Z_STREAM_END:1,Z_NEED_DICT:2,Z_ERRNO:-1,Z_STREAM_ERROR:-2,Z_DATA_ERROR:-3,Z_BUF_ERROR:-5,Z_NO_COMPRESSION:0,Z_BEST_SPEED:1,Z_BEST_COMPRESSION:9,Z_DEFAULT_COMPRESSION:-1,Z_FILTERED:1,Z_HUFFMAN_ONLY:2,Z_RLE:3,Z_FIXED:4,Z_DEFAULT_STRATEGY:0,Z_BINARY:0,Z_TEXT:1,Z_UNKNOWN:2,Z_DEFLATED:8}},{}],45:[function(R,M,S){"use strict";var h=(function(){for(var f,r=[],c=0;c<256;c++){f=c;for(var w=0;w<8;w++)f=1&f?3988292384^f>>>1:f>>>1;r[c]=f}return r})();M.exports=function(f,r,c,w){var z=h,k=w+c;f^=-1;for(var _=w;_<k;_++)f=f>>>8^z[255&(f^r[_])];return-1^f}},{}],46:[function(R,M,S){"use strict";var h,f=R("../utils/common"),r=R("./trees"),c=R("./adler32"),w=R("./crc32"),z=R("./messages"),k=0,_=4,o=0,v=-2,i=-1,m=4,d=2,x=8,E=9,A=286,C=30,$=19,T=2*A+1,H=15,P=3,Y=258,ce=Y+P+1,y=42,B=113,t=1,N=2,pe=3,Z=4;function ge(n,W){return n.msg=z[W],W}function J(n){return(n<<1)-(4<n?9:0)}function be(n){for(var W=n.length;0<=--W;)n[W]=0}function F(n){var W=n.state,D=W.pending;D>n.avail_out&&(D=n.avail_out),D!==0&&(f.arraySet(n.output,W.pending_buf,W.pending_out,D,n.next_out),n.next_out+=D,W.pending_out+=D,n.total_out+=D,n.avail_out-=D,W.pending-=D,W.pending===0&&(W.pending_out=0))}function O(n,W){r._tr_flush_block(n,0<=n.block_start?n.block_start:-1,n.strstart-n.block_start,W),n.block_start=n.strstart,F(n.strm)}function le(n,W){n.pending_buf[n.pending++]=W}function ee(n,W){n.pending_buf[n.pending++]=W>>>8&255,n.pending_buf[n.pending++]=255&W}function X(n,W){var D,b,l=n.max_chain_length,j=n.strstart,q=n.prev_length,U=n.nice_match,I=n.strstart>n.w_size-ce?n.strstart-(n.w_size-ce):0,G=n.window,ue=n.w_mask,V=n.prev,se=n.strstart+Y,Ce=G[j+q-1],Re=G[j+q];n.prev_length>=n.good_match&&(l>>=2),U>n.lookahead&&(U=n.lookahead);do if(G[(D=W)+q]===Re&&G[D+q-1]===Ce&&G[D]===G[j]&&G[++D]===G[j+1]){j+=2,D++;do;while(G[++j]===G[++D]&&G[++j]===G[++D]&&G[++j]===G[++D]&&G[++j]===G[++D]&&G[++j]===G[++D]&&G[++j]===G[++D]&&G[++j]===G[++D]&&G[++j]===G[++D]&&j<se);if(b=Y-(se-j),j=se-Y,q<b){if(n.match_start=W,U<=(q=b))break;Ce=G[j+q-1],Re=G[j+q]}}while((W=V[W&ue])>I&&--l!=0);return q<=n.lookahead?q:n.lookahead}function Oe(n){var W,D,b,l,j,q,U,I,G,ue,V=n.w_size;do{if(l=n.window_size-n.lookahead-n.strstart,n.strstart>=V+(V-ce)){for(f.arraySet(n.window,n.window,V,V,0),n.match_start-=V,n.strstart-=V,n.block_start-=V,W=D=n.hash_size;b=n.head[--W],n.head[W]=V<=b?b-V:0,--D;);for(W=D=V;b=n.prev[--W],n.prev[W]=V<=b?b-V:0,--D;);l+=V}if(n.strm.avail_in===0)break;if(q=n.strm,U=n.window,I=n.strstart+n.lookahead,ue=void 0,(G=l)<(ue=q.avail_in)&&(ue=G),D=ue===0?0:(q.avail_in-=ue,f.arraySet(U,q.input,q.next_in,ue,I),q.state.wrap===1?q.adler=c(q.adler,U,ue,I):q.state.wrap===2&&(q.adler=w(q.adler,U,ue,I)),q.next_in+=ue,q.total_in+=ue,ue),n.lookahead+=D,n.lookahead+n.insert>=P)for(j=n.strstart-n.insert,n.ins_h=n.window[j],n.ins_h=(n.ins_h<<n.hash_shift^n.window[j+1])&n.hash_mask;n.insert&&(n.ins_h=(n.ins_h<<n.hash_shift^n.window[j+P-1])&n.hash_mask,n.prev[j&n.w_mask]=n.head[n.ins_h],n.head[n.ins_h]=j,j++,n.insert--,!(n.lookahead+n.insert<P)););}while(n.lookahead<ce&&n.strm.avail_in!==0)}function Me(n,W){for(var D,b;;){if(n.lookahead<ce){if(Oe(n),n.lookahead<ce&&W===k)return t;if(n.lookahead===0)break}if(D=0,n.lookahead>=P&&(n.ins_h=(n.ins_h<<n.hash_shift^n.window[n.strstart+P-1])&n.hash_mask,D=n.prev[n.strstart&n.w_mask]=n.head[n.ins_h],n.head[n.ins_h]=n.strstart),D!==0&&n.strstart-D<=n.w_size-ce&&(n.match_length=X(n,D)),n.match_length>=P)if(b=r._tr_tally(n,n.strstart-n.match_start,n.match_length-P),n.lookahead-=n.match_length,n.match_length<=n.max_lazy_match&&n.lookahead>=P){for(n.match_length--;n.strstart++,n.ins_h=(n.ins_h<<n.hash_shift^n.window[n.strstart+P-1])&n.hash_mask,D=n.prev[n.strstart&n.w_mask]=n.head[n.ins_h],n.head[n.ins_h]=n.strstart,--n.match_length!=0;);n.strstart++}else n.strstart+=n.match_length,n.match_length=0,n.ins_h=n.window[n.strstart],n.ins_h=(n.ins_h<<n.hash_shift^n.window[n.strstart+1])&n.hash_mask;else b=r._tr_tally(n,0,n.window[n.strstart]),n.lookahead--,n.strstart++;if(b&&(O(n,!1),n.strm.avail_out===0))return t}return n.insert=n.strstart<P-1?n.strstart:P-1,W===_?(O(n,!0),n.strm.avail_out===0?pe:Z):n.last_lit&&(O(n,!1),n.strm.avail_out===0)?t:N}function ke(n,W){for(var D,b,l;;){if(n.lookahead<ce){if(Oe(n),n.lookahead<ce&&W===k)return t;if(n.lookahead===0)break}if(D=0,n.lookahead>=P&&(n.ins_h=(n.ins_h<<n.hash_shift^n.window[n.strstart+P-1])&n.hash_mask,D=n.prev[n.strstart&n.w_mask]=n.head[n.ins_h],n.head[n.ins_h]=n.strstart),n.prev_length=n.match_length,n.prev_match=n.match_start,n.match_length=P-1,D!==0&&n.prev_length<n.max_lazy_match&&n.strstart-D<=n.w_size-ce&&(n.match_length=X(n,D),n.match_length<=5&&(n.strategy===1||n.match_length===P&&4096<n.strstart-n.match_start)&&(n.match_length=P-1)),n.prev_length>=P&&n.match_length<=n.prev_length){for(l=n.strstart+n.lookahead-P,b=r._tr_tally(n,n.strstart-1-n.prev_match,n.prev_length-P),n.lookahead-=n.prev_length-1,n.prev_length-=2;++n.strstart<=l&&(n.ins_h=(n.ins_h<<n.hash_shift^n.window[n.strstart+P-1])&n.hash_mask,D=n.prev[n.strstart&n.w_mask]=n.head[n.ins_h],n.head[n.ins_h]=n.strstart),--n.prev_length!=0;);if(n.match_available=0,n.match_length=P-1,n.strstart++,b&&(O(n,!1),n.strm.avail_out===0))return t}else if(n.match_available){if((b=r._tr_tally(n,0,n.window[n.strstart-1]))&&O(n,!1),n.strstart++,n.lookahead--,n.strm.avail_out===0)return t}else n.match_available=1,n.strstart++,n.lookahead--}return n.match_available&&(b=r._tr_tally(n,0,n.window[n.strstart-1]),n.match_available=0),n.insert=n.strstart<P-1?n.strstart:P-1,W===_?(O(n,!0),n.strm.avail_out===0?pe:Z):n.last_lit&&(O(n,!1),n.strm.avail_out===0)?t:N}function _e(n,W,D,b,l){this.good_length=n,this.max_lazy=W,this.nice_length=D,this.max_chain=b,this.func=l}function Pe(){this.strm=null,this.status=0,this.pending_buf=null,this.pending_buf_size=0,this.pending_out=0,this.pending=0,this.wrap=0,this.gzhead=null,this.gzindex=0,this.method=x,this.last_flush=-1,this.w_size=0,this.w_bits=0,this.w_mask=0,this.window=null,this.window_size=0,this.prev=null,this.head=null,this.ins_h=0,this.hash_size=0,this.hash_bits=0,this.hash_mask=0,this.hash_shift=0,this.block_start=0,this.match_length=0,this.prev_match=0,this.match_available=0,this.strstart=0,this.match_start=0,this.lookahead=0,this.prev_length=0,this.max_chain_length=0,this.max_lazy_match=0,this.level=0,this.strategy=0,this.good_match=0,this.nice_match=0,this.dyn_ltree=new f.Buf16(2*T),this.dyn_dtree=new f.Buf16(2*(2*C+1)),this.bl_tree=new f.Buf16(2*(2*$+1)),be(this.dyn_ltree),be(this.dyn_dtree),be(this.bl_tree),this.l_desc=null,this.d_desc=null,this.bl_desc=null,this.bl_count=new f.Buf16(H+1),this.heap=new f.Buf16(2*A+1),be(this.heap),this.heap_len=0,this.heap_max=0,this.depth=new f.Buf16(2*A+1),be(this.depth),this.l_buf=0,this.lit_bufsize=0,this.last_lit=0,this.d_buf=0,this.opt_len=0,this.static_len=0,this.matches=0,this.insert=0,this.bi_buf=0,this.bi_valid=0}function Fe(n){var W;return n&&n.state?(n.total_in=n.total_out=0,n.data_type=d,(W=n.state).pending=0,W.pending_out=0,W.wrap<0&&(W.wrap=-W.wrap),W.status=W.wrap?y:B,n.adler=W.wrap===2?0:1,W.last_flush=k,r._tr_init(W),o):ge(n,v)}function qe(n){var W=Fe(n);return W===o&&(function(D){D.window_size=2*D.w_size,be(D.head),D.max_lazy_match=h[D.level].max_lazy,D.good_match=h[D.level].good_length,D.nice_match=h[D.level].nice_length,D.max_chain_length=h[D.level].max_chain,D.strstart=0,D.block_start=0,D.lookahead=0,D.insert=0,D.match_length=D.prev_length=P-1,D.match_available=0,D.ins_h=0})(n.state),W}function $e(n,W,D,b,l,j){if(!n)return v;var q=1;if(W===i&&(W=6),b<0?(q=0,b=-b):15<b&&(q=2,b-=16),l<1||E<l||D!==x||b<8||15<b||W<0||9<W||j<0||m<j)return ge(n,v);b===8&&(b=9);var U=new Pe;return(n.state=U).strm=n,U.wrap=q,U.gzhead=null,U.w_bits=b,U.w_size=1<<U.w_bits,U.w_mask=U.w_size-1,U.hash_bits=l+7,U.hash_size=1<<U.hash_bits,U.hash_mask=U.hash_size-1,U.hash_shift=~~((U.hash_bits+P-1)/P),U.window=new f.Buf8(2*U.w_size),U.head=new f.Buf16(U.hash_size),U.prev=new f.Buf16(U.w_size),U.lit_bufsize=1<<l+6,U.pending_buf_size=4*U.lit_bufsize,U.pending_buf=new f.Buf8(U.pending_buf_size),U.d_buf=1*U.lit_bufsize,U.l_buf=3*U.lit_bufsize,U.level=W,U.strategy=j,U.method=D,qe(n)}h=[new _e(0,0,0,0,(function(n,W){var D=65535;for(D>n.pending_buf_size-5&&(D=n.pending_buf_size-5);;){if(n.lookahead<=1){if(Oe(n),n.lookahead===0&&W===k)return t;if(n.lookahead===0)break}n.strstart+=n.lookahead,n.lookahead=0;var b=n.block_start+D;if((n.strstart===0||n.strstart>=b)&&(n.lookahead=n.strstart-b,n.strstart=b,O(n,!1),n.strm.avail_out===0)||n.strstart-n.block_start>=n.w_size-ce&&(O(n,!1),n.strm.avail_out===0))return t}return n.insert=0,W===_?(O(n,!0),n.strm.avail_out===0?pe:Z):(n.strstart>n.block_start&&(O(n,!1),n.strm.avail_out),t)})),new _e(4,4,8,4,Me),new _e(4,5,16,8,Me),new _e(4,6,32,32,Me),new _e(4,4,16,16,ke),new _e(8,16,32,32,ke),new _e(8,16,128,128,ke),new _e(8,32,128,256,ke),new _e(32,128,258,1024,ke),new _e(32,258,258,4096,ke)],S.deflateInit=function(n,W){return $e(n,W,x,15,8,0)},S.deflateInit2=$e,S.deflateReset=qe,S.deflateResetKeep=Fe,S.deflateSetHeader=function(n,W){return n&&n.state?n.state.wrap!==2?v:(n.state.gzhead=W,o):v},S.deflate=function(n,W){var D,b,l,j;if(!n||!n.state||5<W||W<0)return n?ge(n,v):v;if(b=n.state,!n.output||!n.input&&n.avail_in!==0||b.status===666&&W!==_)return ge(n,n.avail_out===0?-5:v);if(b.strm=n,D=b.last_flush,b.last_flush=W,b.status===y)if(b.wrap===2)n.adler=0,le(b,31),le(b,139),le(b,8),b.gzhead?(le(b,(b.gzhead.text?1:0)+(b.gzhead.hcrc?2:0)+(b.gzhead.extra?4:0)+(b.gzhead.name?8:0)+(b.gzhead.comment?16:0)),le(b,255&b.gzhead.time),le(b,b.gzhead.time>>8&255),le(b,b.gzhead.time>>16&255),le(b,b.gzhead.time>>24&255),le(b,b.level===9?2:2<=b.strategy||b.level<2?4:0),le(b,255&b.gzhead.os),b.gzhead.extra&&b.gzhead.extra.length&&(le(b,255&b.gzhead.extra.length),le(b,b.gzhead.extra.length>>8&255)),b.gzhead.hcrc&&(n.adler=w(n.adler,b.pending_buf,b.pending,0)),b.gzindex=0,b.status=69):(le(b,0),le(b,0),le(b,0),le(b,0),le(b,0),le(b,b.level===9?2:2<=b.strategy||b.level<2?4:0),le(b,3),b.status=B);else{var q=x+(b.w_bits-8<<4)<<8;q|=(2<=b.strategy||b.level<2?0:b.level<6?1:b.level===6?2:3)<<6,b.strstart!==0&&(q|=32),q+=31-q%31,b.status=B,ee(b,q),b.strstart!==0&&(ee(b,n.adler>>>16),ee(b,65535&n.adler)),n.adler=1}if(b.status===69)if(b.gzhead.extra){for(l=b.pending;b.gzindex<(65535&b.gzhead.extra.length)&&(b.pending!==b.pending_buf_size||(b.gzhead.hcrc&&b.pending>l&&(n.adler=w(n.adler,b.pending_buf,b.pending-l,l)),F(n),l=b.pending,b.pending!==b.pending_buf_size));)le(b,255&b.gzhead.extra[b.gzindex]),b.gzindex++;b.gzhead.hcrc&&b.pending>l&&(n.adler=w(n.adler,b.pending_buf,b.pending-l,l)),b.gzindex===b.gzhead.extra.length&&(b.gzindex=0,b.status=73)}else b.status=73;if(b.status===73)if(b.gzhead.name){l=b.pending;do{if(b.pending===b.pending_buf_size&&(b.gzhead.hcrc&&b.pending>l&&(n.adler=w(n.adler,b.pending_buf,b.pending-l,l)),F(n),l=b.pending,b.pending===b.pending_buf_size)){j=1;break}j=b.gzindex<b.gzhead.name.length?255&b.gzhead.name.charCodeAt(b.gzindex++):0,le(b,j)}while(j!==0);b.gzhead.hcrc&&b.pending>l&&(n.adler=w(n.adler,b.pending_buf,b.pending-l,l)),j===0&&(b.gzindex=0,b.status=91)}else b.status=91;if(b.status===91)if(b.gzhead.comment){l=b.pending;do{if(b.pending===b.pending_buf_size&&(b.gzhead.hcrc&&b.pending>l&&(n.adler=w(n.adler,b.pending_buf,b.pending-l,l)),F(n),l=b.pending,b.pending===b.pending_buf_size)){j=1;break}j=b.gzindex<b.gzhead.comment.length?255&b.gzhead.comment.charCodeAt(b.gzindex++):0,le(b,j)}while(j!==0);b.gzhead.hcrc&&b.pending>l&&(n.adler=w(n.adler,b.pending_buf,b.pending-l,l)),j===0&&(b.status=103)}else b.status=103;if(b.status===103&&(b.gzhead.hcrc?(b.pending+2>b.pending_buf_size&&F(n),b.pending+2<=b.pending_buf_size&&(le(b,255&n.adler),le(b,n.adler>>8&255),n.adler=0,b.status=B)):b.status=B),b.pending!==0){if(F(n),n.avail_out===0)return b.last_flush=-1,o}else if(n.avail_in===0&&J(W)<=J(D)&&W!==_)return ge(n,-5);if(b.status===666&&n.avail_in!==0)return ge(n,-5);if(n.avail_in!==0||b.lookahead!==0||W!==k&&b.status!==666){var U=b.strategy===2?(function(I,G){for(var ue;;){if(I.lookahead===0&&(Oe(I),I.lookahead===0)){if(G===k)return t;break}if(I.match_length=0,ue=r._tr_tally(I,0,I.window[I.strstart]),I.lookahead--,I.strstart++,ue&&(O(I,!1),I.strm.avail_out===0))return t}return I.insert=0,G===_?(O(I,!0),I.strm.avail_out===0?pe:Z):I.last_lit&&(O(I,!1),I.strm.avail_out===0)?t:N})(b,W):b.strategy===3?(function(I,G){for(var ue,V,se,Ce,Re=I.window;;){if(I.lookahead<=Y){if(Oe(I),I.lookahead<=Y&&G===k)return t;if(I.lookahead===0)break}if(I.match_length=0,I.lookahead>=P&&0<I.strstart&&(V=Re[se=I.strstart-1])===Re[++se]&&V===Re[++se]&&V===Re[++se]){Ce=I.strstart+Y;do;while(V===Re[++se]&&V===Re[++se]&&V===Re[++se]&&V===Re[++se]&&V===Re[++se]&&V===Re[++se]&&V===Re[++se]&&V===Re[++se]&&se<Ce);I.match_length=Y-(Ce-se),I.match_length>I.lookahead&&(I.match_length=I.lookahead)}if(I.match_length>=P?(ue=r._tr_tally(I,1,I.match_length-P),I.lookahead-=I.match_length,I.strstart+=I.match_length,I.match_length=0):(ue=r._tr_tally(I,0,I.window[I.strstart]),I.lookahead--,I.strstart++),ue&&(O(I,!1),I.strm.avail_out===0))return t}return I.insert=0,G===_?(O(I,!0),I.strm.avail_out===0?pe:Z):I.last_lit&&(O(I,!1),I.strm.avail_out===0)?t:N})(b,W):h[b.level].func(b,W);if(U!==pe&&U!==Z||(b.status=666),U===t||U===pe)return n.avail_out===0&&(b.last_flush=-1),o;if(U===N&&(W===1?r._tr_align(b):W!==5&&(r._tr_stored_block(b,0,0,!1),W===3&&(be(b.head),b.lookahead===0&&(b.strstart=0,b.block_start=0,b.insert=0))),F(n),n.avail_out===0))return b.last_flush=-1,o}return W!==_?o:b.wrap<=0?1:(b.wrap===2?(le(b,255&n.adler),le(b,n.adler>>8&255),le(b,n.adler>>16&255),le(b,n.adler>>24&255),le(b,255&n.total_in),le(b,n.total_in>>8&255),le(b,n.total_in>>16&255),le(b,n.total_in>>24&255)):(ee(b,n.adler>>>16),ee(b,65535&n.adler)),F(n),0<b.wrap&&(b.wrap=-b.wrap),b.pending!==0?o:1)},S.deflateEnd=function(n){var W;return n&&n.state?(W=n.state.status)!==y&&W!==69&&W!==73&&W!==91&&W!==103&&W!==B&&W!==666?ge(n,v):(n.state=null,W===B?ge(n,-3):o):v},S.deflateSetDictionary=function(n,W){var D,b,l,j,q,U,I,G,ue=W.length;if(!n||!n.state||(j=(D=n.state).wrap)===2||j===1&&D.status!==y||D.lookahead)return v;for(j===1&&(n.adler=c(n.adler,W,ue,0)),D.wrap=0,ue>=D.w_size&&(j===0&&(be(D.head),D.strstart=0,D.block_start=0,D.insert=0),G=new f.Buf8(D.w_size),f.arraySet(G,W,ue-D.w_size,D.w_size,0),W=G,ue=D.w_size),q=n.avail_in,U=n.next_in,I=n.input,n.avail_in=ue,n.next_in=0,n.input=W,Oe(D);D.lookahead>=P;){for(b=D.strstart,l=D.lookahead-(P-1);D.ins_h=(D.ins_h<<D.hash_shift^D.window[b+P-1])&D.hash_mask,D.prev[b&D.w_mask]=D.head[D.ins_h],D.head[D.ins_h]=b,b++,--l;);D.strstart=b,D.lookahead=P-1,Oe(D)}return D.strstart+=D.lookahead,D.block_start=D.strstart,D.insert=D.lookahead,D.lookahead=0,D.match_length=D.prev_length=P-1,D.match_available=0,n.next_in=U,n.input=I,n.avail_in=q,D.wrap=j,o},S.deflateInfo="pako deflate (from Nodeca project)"},{"../utils/common":41,"./adler32":43,"./crc32":45,"./messages":51,"./trees":52}],47:[function(R,M,S){"use strict";M.exports=function(){this.text=0,this.time=0,this.xflags=0,this.os=0,this.extra=null,this.extra_len=0,this.name="",this.comment="",this.hcrc=0,this.done=!1}},{}],48:[function(R,M,S){"use strict";M.exports=function(h,f){var r,c,w,z,k,_,o,v,i,m,d,x,E,A,C,$,T,H,P,Y,ce,y,B,t,N;r=h.state,c=h.next_in,t=h.input,w=c+(h.avail_in-5),z=h.next_out,N=h.output,k=z-(f-h.avail_out),_=z+(h.avail_out-257),o=r.dmax,v=r.wsize,i=r.whave,m=r.wnext,d=r.window,x=r.hold,E=r.bits,A=r.lencode,C=r.distcode,$=(1<<r.lenbits)-1,T=(1<<r.distbits)-1;e:do{E<15&&(x+=t[c++]<<E,E+=8,x+=t[c++]<<E,E+=8),H=A[x&$];u:for(;;){if(x>>>=P=H>>>24,E-=P,(P=H>>>16&255)==0)N[z++]=65535&H;else{if(!(16&P)){if((64&P)==0){H=A[(65535&H)+(x&(1<<P)-1)];continue u}if(32&P){r.mode=12;break e}h.msg="invalid literal/length code",r.mode=30;break e}Y=65535&H,(P&=15)&&(E<P&&(x+=t[c++]<<E,E+=8),Y+=x&(1<<P)-1,x>>>=P,E-=P),E<15&&(x+=t[c++]<<E,E+=8,x+=t[c++]<<E,E+=8),H=C[x&T];n:for(;;){if(x>>>=P=H>>>24,E-=P,!(16&(P=H>>>16&255))){if((64&P)==0){H=C[(65535&H)+(x&(1<<P)-1)];continue n}h.msg="invalid distance code",r.mode=30;break e}if(ce=65535&H,E<(P&=15)&&(x+=t[c++]<<E,(E+=8)<P&&(x+=t[c++]<<E,E+=8)),o<(ce+=x&(1<<P)-1)){h.msg="invalid distance too far back",r.mode=30;break e}if(x>>>=P,E-=P,(P=z-k)<ce){if(i<(P=ce-P)&&r.sane){h.msg="invalid distance too far back",r.mode=30;break e}if(B=d,(y=0)===m){if(y+=v-P,P<Y){for(Y-=P;N[z++]=d[y++],--P;);y=z-ce,B=N}}else if(m<P){if(y+=v+m-P,(P-=m)<Y){for(Y-=P;N[z++]=d[y++],--P;);if(y=0,m<Y){for(Y-=P=m;N[z++]=d[y++],--P;);y=z-ce,B=N}}}else if(y+=m-P,P<Y){for(Y-=P;N[z++]=d[y++],--P;);y=z-ce,B=N}for(;2<Y;)N[z++]=B[y++],N[z++]=B[y++],N[z++]=B[y++],Y-=3;Y&&(N[z++]=B[y++],1<Y&&(N[z++]=B[y++]))}else{for(y=z-ce;N[z++]=N[y++],N[z++]=N[y++],N[z++]=N[y++],2<(Y-=3););Y&&(N[z++]=N[y++],1<Y&&(N[z++]=N[y++]))}break}}break}}while(c<w&&z<_);c-=Y=E>>3,x&=(1<<(E-=Y<<3))-1,h.next_in=c,h.next_out=z,h.avail_in=c<w?w-c+5:5-(c-w),h.avail_out=z<_?_-z+257:257-(z-_),r.hold=x,r.bits=E}},{}],49:[function(R,M,S){"use strict";var h=R("../utils/common"),f=R("./adler32"),r=R("./crc32"),c=R("./inffast"),w=R("./inftrees"),z=1,k=2,_=0,o=-2,v=1,i=852,m=592;function d(y){return(y>>>24&255)+(y>>>8&65280)+((65280&y)<<8)+((255&y)<<24)}function x(){this.mode=0,this.last=!1,this.wrap=0,this.havedict=!1,this.flags=0,this.dmax=0,this.check=0,this.total=0,this.head=null,this.wbits=0,this.wsize=0,this.whave=0,this.wnext=0,this.window=null,this.hold=0,this.bits=0,this.length=0,this.offset=0,this.extra=0,this.lencode=null,this.distcode=null,this.lenbits=0,this.distbits=0,this.ncode=0,this.nlen=0,this.ndist=0,this.have=0,this.next=null,this.lens=new h.Buf16(320),this.work=new h.Buf16(288),this.lendyn=null,this.distdyn=null,this.sane=0,this.back=0,this.was=0}function E(y){var B;return y&&y.state?(B=y.state,y.total_in=y.total_out=B.total=0,y.msg="",B.wrap&&(y.adler=1&B.wrap),B.mode=v,B.last=0,B.havedict=0,B.dmax=32768,B.head=null,B.hold=0,B.bits=0,B.lencode=B.lendyn=new h.Buf32(i),B.distcode=B.distdyn=new h.Buf32(m),B.sane=1,B.back=-1,_):o}function A(y){var B;return y&&y.state?((B=y.state).wsize=0,B.whave=0,B.wnext=0,E(y)):o}function C(y,B){var t,N;return y&&y.state?(N=y.state,B<0?(t=0,B=-B):(t=1+(B>>4),B<48&&(B&=15)),B&&(B<8||15<B)?o:(N.window!==null&&N.wbits!==B&&(N.window=null),N.wrap=t,N.wbits=B,A(y))):o}function $(y,B){var t,N;return y?(N=new x,(y.state=N).window=null,(t=C(y,B))!==_&&(y.state=null),t):o}var T,H,P=!0;function Y(y){if(P){var B;for(T=new h.Buf32(512),H=new h.Buf32(32),B=0;B<144;)y.lens[B++]=8;for(;B<256;)y.lens[B++]=9;for(;B<280;)y.lens[B++]=7;for(;B<288;)y.lens[B++]=8;for(w(z,y.lens,0,288,T,0,y.work,{bits:9}),B=0;B<32;)y.lens[B++]=5;w(k,y.lens,0,32,H,0,y.work,{bits:5}),P=!1}y.lencode=T,y.lenbits=9,y.distcode=H,y.distbits=5}function ce(y,B,t,N){var pe,Z=y.state;return Z.window===null&&(Z.wsize=1<<Z.wbits,Z.wnext=0,Z.whave=0,Z.window=new h.Buf8(Z.wsize)),N>=Z.wsize?(h.arraySet(Z.window,B,t-Z.wsize,Z.wsize,0),Z.wnext=0,Z.whave=Z.wsize):(N<(pe=Z.wsize-Z.wnext)&&(pe=N),h.arraySet(Z.window,B,t-N,pe,Z.wnext),(N-=pe)?(h.arraySet(Z.window,B,t-N,N,0),Z.wnext=N,Z.whave=Z.wsize):(Z.wnext+=pe,Z.wnext===Z.wsize&&(Z.wnext=0),Z.whave<Z.wsize&&(Z.whave+=pe))),0}S.inflateReset=A,S.inflateReset2=C,S.inflateResetKeep=E,S.inflateInit=function(y){return $(y,15)},S.inflateInit2=$,S.inflate=function(y,B){var t,N,pe,Z,ge,J,be,F,O,le,ee,X,Oe,Me,ke,_e,Pe,Fe,qe,$e,n,W,D,b,l=0,j=new h.Buf8(4),q=[16,17,18,0,8,7,9,6,10,5,11,4,12,3,13,2,14,1,15];if(!y||!y.state||!y.output||!y.input&&y.avail_in!==0)return o;(t=y.state).mode===12&&(t.mode=13),ge=y.next_out,pe=y.output,be=y.avail_out,Z=y.next_in,N=y.input,J=y.avail_in,F=t.hold,O=t.bits,le=J,ee=be,W=_;e:for(;;)switch(t.mode){case v:if(t.wrap===0){t.mode=13;break}for(;O<16;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if(2&t.wrap&&F===35615){j[t.check=0]=255&F,j[1]=F>>>8&255,t.check=r(t.check,j,2,0),O=F=0,t.mode=2;break}if(t.flags=0,t.head&&(t.head.done=!1),!(1&t.wrap)||(((255&F)<<8)+(F>>8))%31){y.msg="incorrect header check",t.mode=30;break}if((15&F)!=8){y.msg="unknown compression method",t.mode=30;break}if(O-=4,n=8+(15&(F>>>=4)),t.wbits===0)t.wbits=n;else if(n>t.wbits){y.msg="invalid window size",t.mode=30;break}t.dmax=1<<n,y.adler=t.check=1,t.mode=512&F?10:12,O=F=0;break;case 2:for(;O<16;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if(t.flags=F,(255&t.flags)!=8){y.msg="unknown compression method",t.mode=30;break}if(57344&t.flags){y.msg="unknown header flags set",t.mode=30;break}t.head&&(t.head.text=F>>8&1),512&t.flags&&(j[0]=255&F,j[1]=F>>>8&255,t.check=r(t.check,j,2,0)),O=F=0,t.mode=3;case 3:for(;O<32;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}t.head&&(t.head.time=F),512&t.flags&&(j[0]=255&F,j[1]=F>>>8&255,j[2]=F>>>16&255,j[3]=F>>>24&255,t.check=r(t.check,j,4,0)),O=F=0,t.mode=4;case 4:for(;O<16;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}t.head&&(t.head.xflags=255&F,t.head.os=F>>8),512&t.flags&&(j[0]=255&F,j[1]=F>>>8&255,t.check=r(t.check,j,2,0)),O=F=0,t.mode=5;case 5:if(1024&t.flags){for(;O<16;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}t.length=F,t.head&&(t.head.extra_len=F),512&t.flags&&(j[0]=255&F,j[1]=F>>>8&255,t.check=r(t.check,j,2,0)),O=F=0}else t.head&&(t.head.extra=null);t.mode=6;case 6:if(1024&t.flags&&(J<(X=t.length)&&(X=J),X&&(t.head&&(n=t.head.extra_len-t.length,t.head.extra||(t.head.extra=new Array(t.head.extra_len)),h.arraySet(t.head.extra,N,Z,X,n)),512&t.flags&&(t.check=r(t.check,N,X,Z)),J-=X,Z+=X,t.length-=X),t.length))break e;t.length=0,t.mode=7;case 7:if(2048&t.flags){if(J===0)break e;for(X=0;n=N[Z+X++],t.head&&n&&t.length<65536&&(t.head.name+=String.fromCharCode(n)),n&&X<J;);if(512&t.flags&&(t.check=r(t.check,N,X,Z)),J-=X,Z+=X,n)break e}else t.head&&(t.head.name=null);t.length=0,t.mode=8;case 8:if(4096&t.flags){if(J===0)break e;for(X=0;n=N[Z+X++],t.head&&n&&t.length<65536&&(t.head.comment+=String.fromCharCode(n)),n&&X<J;);if(512&t.flags&&(t.check=r(t.check,N,X,Z)),J-=X,Z+=X,n)break e}else t.head&&(t.head.comment=null);t.mode=9;case 9:if(512&t.flags){for(;O<16;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if(F!==(65535&t.check)){y.msg="header crc mismatch",t.mode=30;break}O=F=0}t.head&&(t.head.hcrc=t.flags>>9&1,t.head.done=!0),y.adler=t.check=0,t.mode=12;break;case 10:for(;O<32;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}y.adler=t.check=d(F),O=F=0,t.mode=11;case 11:if(t.havedict===0)return y.next_out=ge,y.avail_out=be,y.next_in=Z,y.avail_in=J,t.hold=F,t.bits=O,2;y.adler=t.check=1,t.mode=12;case 12:if(B===5||B===6)break e;case 13:if(t.last){F>>>=7&O,O-=7&O,t.mode=27;break}for(;O<3;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}switch(t.last=1&F,O-=1,3&(F>>>=1)){case 0:t.mode=14;break;case 1:if(Y(t),t.mode=20,B!==6)break;F>>>=2,O-=2;break e;case 2:t.mode=17;break;case 3:y.msg="invalid block type",t.mode=30}F>>>=2,O-=2;break;case 14:for(F>>>=7&O,O-=7&O;O<32;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if((65535&F)!=(F>>>16^65535)){y.msg="invalid stored block lengths",t.mode=30;break}if(t.length=65535&F,O=F=0,t.mode=15,B===6)break e;case 15:t.mode=16;case 16:if(X=t.length){if(J<X&&(X=J),be<X&&(X=be),X===0)break e;h.arraySet(pe,N,Z,X,ge),J-=X,Z+=X,be-=X,ge+=X,t.length-=X;break}t.mode=12;break;case 17:for(;O<14;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if(t.nlen=257+(31&F),F>>>=5,O-=5,t.ndist=1+(31&F),F>>>=5,O-=5,t.ncode=4+(15&F),F>>>=4,O-=4,286<t.nlen||30<t.ndist){y.msg="too many length or distance symbols",t.mode=30;break}t.have=0,t.mode=18;case 18:for(;t.have<t.ncode;){for(;O<3;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}t.lens[q[t.have++]]=7&F,F>>>=3,O-=3}for(;t.have<19;)t.lens[q[t.have++]]=0;if(t.lencode=t.lendyn,t.lenbits=7,D={bits:t.lenbits},W=w(0,t.lens,0,19,t.lencode,0,t.work,D),t.lenbits=D.bits,W){y.msg="invalid code lengths set",t.mode=30;break}t.have=0,t.mode=19;case 19:for(;t.have<t.nlen+t.ndist;){for(;_e=(l=t.lencode[F&(1<<t.lenbits)-1])>>>16&255,Pe=65535&l,!((ke=l>>>24)<=O);){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if(Pe<16)F>>>=ke,O-=ke,t.lens[t.have++]=Pe;else{if(Pe===16){for(b=ke+2;O<b;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if(F>>>=ke,O-=ke,t.have===0){y.msg="invalid bit length repeat",t.mode=30;break}n=t.lens[t.have-1],X=3+(3&F),F>>>=2,O-=2}else if(Pe===17){for(b=ke+3;O<b;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}O-=ke,n=0,X=3+(7&(F>>>=ke)),F>>>=3,O-=3}else{for(b=ke+7;O<b;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}O-=ke,n=0,X=11+(127&(F>>>=ke)),F>>>=7,O-=7}if(t.have+X>t.nlen+t.ndist){y.msg="invalid bit length repeat",t.mode=30;break}for(;X--;)t.lens[t.have++]=n}}if(t.mode===30)break;if(t.lens[256]===0){y.msg="invalid code -- missing end-of-block",t.mode=30;break}if(t.lenbits=9,D={bits:t.lenbits},W=w(z,t.lens,0,t.nlen,t.lencode,0,t.work,D),t.lenbits=D.bits,W){y.msg="invalid literal/lengths set",t.mode=30;break}if(t.distbits=6,t.distcode=t.distdyn,D={bits:t.distbits},W=w(k,t.lens,t.nlen,t.ndist,t.distcode,0,t.work,D),t.distbits=D.bits,W){y.msg="invalid distances set",t.mode=30;break}if(t.mode=20,B===6)break e;case 20:t.mode=21;case 21:if(6<=J&&258<=be){y.next_out=ge,y.avail_out=be,y.next_in=Z,y.avail_in=J,t.hold=F,t.bits=O,c(y,ee),ge=y.next_out,pe=y.output,be=y.avail_out,Z=y.next_in,N=y.input,J=y.avail_in,F=t.hold,O=t.bits,t.mode===12&&(t.back=-1);break}for(t.back=0;_e=(l=t.lencode[F&(1<<t.lenbits)-1])>>>16&255,Pe=65535&l,!((ke=l>>>24)<=O);){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if(_e&&(240&_e)==0){for(Fe=ke,qe=_e,$e=Pe;_e=(l=t.lencode[$e+((F&(1<<Fe+qe)-1)>>Fe)])>>>16&255,Pe=65535&l,!(Fe+(ke=l>>>24)<=O);){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}F>>>=Fe,O-=Fe,t.back+=Fe}if(F>>>=ke,O-=ke,t.back+=ke,t.length=Pe,_e===0){t.mode=26;break}if(32&_e){t.back=-1,t.mode=12;break}if(64&_e){y.msg="invalid literal/length code",t.mode=30;break}t.extra=15&_e,t.mode=22;case 22:if(t.extra){for(b=t.extra;O<b;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}t.length+=F&(1<<t.extra)-1,F>>>=t.extra,O-=t.extra,t.back+=t.extra}t.was=t.length,t.mode=23;case 23:for(;_e=(l=t.distcode[F&(1<<t.distbits)-1])>>>16&255,Pe=65535&l,!((ke=l>>>24)<=O);){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if((240&_e)==0){for(Fe=ke,qe=_e,$e=Pe;_e=(l=t.distcode[$e+((F&(1<<Fe+qe)-1)>>Fe)])>>>16&255,Pe=65535&l,!(Fe+(ke=l>>>24)<=O);){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}F>>>=Fe,O-=Fe,t.back+=Fe}if(F>>>=ke,O-=ke,t.back+=ke,64&_e){y.msg="invalid distance code",t.mode=30;break}t.offset=Pe,t.extra=15&_e,t.mode=24;case 24:if(t.extra){for(b=t.extra;O<b;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}t.offset+=F&(1<<t.extra)-1,F>>>=t.extra,O-=t.extra,t.back+=t.extra}if(t.offset>t.dmax){y.msg="invalid distance too far back",t.mode=30;break}t.mode=25;case 25:if(be===0)break e;if(X=ee-be,t.offset>X){if((X=t.offset-X)>t.whave&&t.sane){y.msg="invalid distance too far back",t.mode=30;break}Oe=X>t.wnext?(X-=t.wnext,t.wsize-X):t.wnext-X,X>t.length&&(X=t.length),Me=t.window}else Me=pe,Oe=ge-t.offset,X=t.length;for(be<X&&(X=be),be-=X,t.length-=X;pe[ge++]=Me[Oe++],--X;);t.length===0&&(t.mode=21);break;case 26:if(be===0)break e;pe[ge++]=t.length,be--,t.mode=21;break;case 27:if(t.wrap){for(;O<32;){if(J===0)break e;J--,F|=N[Z++]<<O,O+=8}if(ee-=be,y.total_out+=ee,t.total+=ee,ee&&(y.adler=t.check=t.flags?r(t.check,pe,ee,ge-ee):f(t.check,pe,ee,ge-ee)),ee=be,(t.flags?F:d(F))!==t.check){y.msg="incorrect data check",t.mode=30;break}O=F=0}t.mode=28;case 28:if(t.wrap&&t.flags){for(;O<32;){if(J===0)break e;J--,F+=N[Z++]<<O,O+=8}if(F!==(4294967295&t.total)){y.msg="incorrect length check",t.mode=30;break}O=F=0}t.mode=29;case 29:W=1;break e;case 30:W=-3;break e;case 31:return-4;default:return o}return y.next_out=ge,y.avail_out=be,y.next_in=Z,y.avail_in=J,t.hold=F,t.bits=O,(t.wsize||ee!==y.avail_out&&t.mode<30&&(t.mode<27||B!==4))&&ce(y,y.output,y.next_out,ee-y.avail_out)?(t.mode=31,-4):(le-=y.avail_in,ee-=y.avail_out,y.total_in+=le,y.total_out+=ee,t.total+=ee,t.wrap&&ee&&(y.adler=t.check=t.flags?r(t.check,pe,ee,y.next_out-ee):f(t.check,pe,ee,y.next_out-ee)),y.data_type=t.bits+(t.last?64:0)+(t.mode===12?128:0)+(t.mode===20||t.mode===15?256:0),(le==0&&ee===0||B===4)&&W===_&&(W=-5),W)},S.inflateEnd=function(y){if(!y||!y.state)return o;var B=y.state;return B.window&&(B.window=null),y.state=null,_},S.inflateGetHeader=function(y,B){var t;return y&&y.state?(2&(t=y.state).wrap)==0?o:((t.head=B).done=!1,_):o},S.inflateSetDictionary=function(y,B){var t,N=B.length;return y&&y.state?(t=y.state).wrap!==0&&t.mode!==11?o:t.mode===11&&f(1,B,N,0)!==t.check?-3:ce(y,B,N,N)?(t.mode=31,-4):(t.havedict=1,_):o},S.inflateInfo="pako inflate (from Nodeca project)"},{"../utils/common":41,"./adler32":43,"./crc32":45,"./inffast":48,"./inftrees":50}],50:[function(R,M,S){"use strict";var h=R("../utils/common"),f=[3,4,5,6,7,8,9,10,11,13,15,17,19,23,27,31,35,43,51,59,67,83,99,115,131,163,195,227,258,0,0],r=[16,16,16,16,16,16,16,16,17,17,17,17,18,18,18,18,19,19,19,19,20,20,20,20,21,21,21,21,16,72,78],c=[1,2,3,4,5,7,9,13,17,25,33,49,65,97,129,193,257,385,513,769,1025,1537,2049,3073,4097,6145,8193,12289,16385,24577,0,0],w=[16,16,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23,24,24,25,25,26,26,27,27,28,28,29,29,64,64];M.exports=function(z,k,_,o,v,i,m,d){var x,E,A,C,$,T,H,P,Y,ce=d.bits,y=0,B=0,t=0,N=0,pe=0,Z=0,ge=0,J=0,be=0,F=0,O=null,le=0,ee=new h.Buf16(16),X=new h.Buf16(16),Oe=null,Me=0;for(y=0;y<=15;y++)ee[y]=0;for(B=0;B<o;B++)ee[k[_+B]]++;for(pe=ce,N=15;1<=N&&ee[N]===0;N--);if(N<pe&&(pe=N),N===0)return v[i++]=20971520,v[i++]=20971520,d.bits=1,0;for(t=1;t<N&&ee[t]===0;t++);for(pe<t&&(pe=t),y=J=1;y<=15;y++)if(J<<=1,(J-=ee[y])<0)return-1;if(0<J&&(z===0||N!==1))return-1;for(X[1]=0,y=1;y<15;y++)X[y+1]=X[y]+ee[y];for(B=0;B<o;B++)k[_+B]!==0&&(m[X[k[_+B]]++]=B);if(T=z===0?(O=Oe=m,19):z===1?(O=f,le-=257,Oe=r,Me-=257,256):(O=c,Oe=w,-1),y=t,$=i,ge=B=F=0,A=-1,C=(be=1<<(Z=pe))-1,z===1&&852<be||z===2&&592<be)return 1;for(;;){for(H=y-ge,Y=m[B]<T?(P=0,m[B]):m[B]>T?(P=Oe[Me+m[B]],O[le+m[B]]):(P=96,0),x=1<<y-ge,t=E=1<<Z;v[$+(F>>ge)+(E-=x)]=H<<24|P<<16|Y|0,E!==0;);for(x=1<<y-1;F&x;)x>>=1;if(x!==0?(F&=x-1,F+=x):F=0,B++,--ee[y]==0){if(y===N)break;y=k[_+m[B]]}if(pe<y&&(F&C)!==A){for(ge===0&&(ge=pe),$+=t,J=1<<(Z=y-ge);Z+ge<N&&!((J-=ee[Z+ge])<=0);)Z++,J<<=1;if(be+=1<<Z,z===1&&852<be||z===2&&592<be)return 1;v[A=F&C]=pe<<24|Z<<16|$-i|0}}return F!==0&&(v[$+F]=y-ge<<24|4194304|0),d.bits=pe,0}},{"../utils/common":41}],51:[function(R,M,S){"use strict";M.exports={2:"need dictionary",1:"stream end",0:"","-1":"file error","-2":"stream error","-3":"data error","-4":"insufficient memory","-5":"buffer error","-6":"incompatible version"}},{}],52:[function(R,M,S){"use strict";var h=R("../utils/common"),f=0,r=1;function c(l){for(var j=l.length;0<=--j;)l[j]=0}var w=0,z=29,k=256,_=k+1+z,o=30,v=19,i=2*_+1,m=15,d=16,x=7,E=256,A=16,C=17,$=18,T=[0,0,0,0,0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5,0],H=[0,0,0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13],P=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,3,7],Y=[16,17,18,0,8,7,9,6,10,5,11,4,12,3,13,2,14,1,15],ce=new Array(2*(_+2));c(ce);var y=new Array(2*o);c(y);var B=new Array(512);c(B);var t=new Array(256);c(t);var N=new Array(z);c(N);var pe,Z,ge,J=new Array(o);function be(l,j,q,U,I){this.static_tree=l,this.extra_bits=j,this.extra_base=q,this.elems=U,this.max_length=I,this.has_stree=l&&l.length}function F(l,j){this.dyn_tree=l,this.max_code=0,this.stat_desc=j}function O(l){return l<256?B[l]:B[256+(l>>>7)]}function le(l,j){l.pending_buf[l.pending++]=255&j,l.pending_buf[l.pending++]=j>>>8&255}function ee(l,j,q){l.bi_valid>d-q?(l.bi_buf|=j<<l.bi_valid&65535,le(l,l.bi_buf),l.bi_buf=j>>d-l.bi_valid,l.bi_valid+=q-d):(l.bi_buf|=j<<l.bi_valid&65535,l.bi_valid+=q)}function X(l,j,q){ee(l,q[2*j],q[2*j+1])}function Oe(l,j){for(var q=0;q|=1&l,l>>>=1,q<<=1,0<--j;);return q>>>1}function Me(l,j,q){var U,I,G=new Array(m+1),ue=0;for(U=1;U<=m;U++)G[U]=ue=ue+q[U-1]<<1;for(I=0;I<=j;I++){var V=l[2*I+1];V!==0&&(l[2*I]=Oe(G[V]++,V))}}function ke(l){var j;for(j=0;j<_;j++)l.dyn_ltree[2*j]=0;for(j=0;j<o;j++)l.dyn_dtree[2*j]=0;for(j=0;j<v;j++)l.bl_tree[2*j]=0;l.dyn_ltree[2*E]=1,l.opt_len=l.static_len=0,l.last_lit=l.matches=0}function _e(l){8<l.bi_valid?le(l,l.bi_buf):0<l.bi_valid&&(l.pending_buf[l.pending++]=l.bi_buf),l.bi_buf=0,l.bi_valid=0}function Pe(l,j,q,U){var I=2*j,G=2*q;return l[I]<l[G]||l[I]===l[G]&&U[j]<=U[q]}function Fe(l,j,q){for(var U=l.heap[q],I=q<<1;I<=l.heap_len&&(I<l.heap_len&&Pe(j,l.heap[I+1],l.heap[I],l.depth)&&I++,!Pe(j,U,l.heap[I],l.depth));)l.heap[q]=l.heap[I],q=I,I<<=1;l.heap[q]=U}function qe(l,j,q){var U,I,G,ue,V=0;if(l.last_lit!==0)for(;U=l.pending_buf[l.d_buf+2*V]<<8|l.pending_buf[l.d_buf+2*V+1],I=l.pending_buf[l.l_buf+V],V++,U===0?X(l,I,j):(X(l,(G=t[I])+k+1,j),(ue=T[G])!==0&&ee(l,I-=N[G],ue),X(l,G=O(--U),q),(ue=H[G])!==0&&ee(l,U-=J[G],ue)),V<l.last_lit;);X(l,E,j)}function $e(l,j){var q,U,I,G=j.dyn_tree,ue=j.stat_desc.static_tree,V=j.stat_desc.has_stree,se=j.stat_desc.elems,Ce=-1;for(l.heap_len=0,l.heap_max=i,q=0;q<se;q++)G[2*q]!==0?(l.heap[++l.heap_len]=Ce=q,l.depth[q]=0):G[2*q+1]=0;for(;l.heap_len<2;)G[2*(I=l.heap[++l.heap_len]=Ce<2?++Ce:0)]=1,l.depth[I]=0,l.opt_len--,V&&(l.static_len-=ue[2*I+1]);for(j.max_code=Ce,q=l.heap_len>>1;1<=q;q--)Fe(l,G,q);for(I=se;q=l.heap[1],l.heap[1]=l.heap[l.heap_len--],Fe(l,G,1),U=l.heap[1],l.heap[--l.heap_max]=q,l.heap[--l.heap_max]=U,G[2*I]=G[2*q]+G[2*U],l.depth[I]=(l.depth[q]>=l.depth[U]?l.depth[q]:l.depth[U])+1,G[2*q+1]=G[2*U+1]=I,l.heap[1]=I++,Fe(l,G,1),2<=l.heap_len;);l.heap[--l.heap_max]=l.heap[1],(function(Re,Le){var eu,Ue,we,ye,re,ru,Ze=Le.dyn_tree,ae=Le.max_code,He=Le.stat_desc.static_tree,Ae=Le.stat_desc.has_stree,Au=Le.stat_desc.extra_bits,vu=Le.stat_desc.extra_base,Ye=Le.stat_desc.max_length,tu=0;for(ye=0;ye<=m;ye++)Re.bl_count[ye]=0;for(Ze[2*Re.heap[Re.heap_max]+1]=0,eu=Re.heap_max+1;eu<i;eu++)Ye<(ye=Ze[2*Ze[2*(Ue=Re.heap[eu])+1]+1]+1)&&(ye=Ye,tu++),Ze[2*Ue+1]=ye,ae<Ue||(Re.bl_count[ye]++,re=0,vu<=Ue&&(re=Au[Ue-vu]),ru=Ze[2*Ue],Re.opt_len+=ru*(ye+re),Ae&&(Re.static_len+=ru*(He[2*Ue+1]+re)));if(tu!==0){do{for(ye=Ye-1;Re.bl_count[ye]===0;)ye--;Re.bl_count[ye]--,Re.bl_count[ye+1]+=2,Re.bl_count[Ye]--,tu-=2}while(0<tu);for(ye=Ye;ye!==0;ye--)for(Ue=Re.bl_count[ye];Ue!==0;)ae<(we=Re.heap[--eu])||(Ze[2*we+1]!==ye&&(Re.opt_len+=(ye-Ze[2*we+1])*Ze[2*we],Ze[2*we+1]=ye),Ue--)}})(l,j),Me(G,Ce,l.bl_count)}function n(l,j,q){var U,I,G=-1,ue=j[1],V=0,se=7,Ce=4;for(ue===0&&(se=138,Ce=3),j[2*(q+1)+1]=65535,U=0;U<=q;U++)I=ue,ue=j[2*(U+1)+1],++V<se&&I===ue||(V<Ce?l.bl_tree[2*I]+=V:I!==0?(I!==G&&l.bl_tree[2*I]++,l.bl_tree[2*A]++):V<=10?l.bl_tree[2*C]++:l.bl_tree[2*$]++,G=I,Ce=(V=0)===ue?(se=138,3):I===ue?(se=6,3):(se=7,4))}function W(l,j,q){var U,I,G=-1,ue=j[1],V=0,se=7,Ce=4;for(ue===0&&(se=138,Ce=3),U=0;U<=q;U++)if(I=ue,ue=j[2*(U+1)+1],!(++V<se&&I===ue)){if(V<Ce)for(;X(l,I,l.bl_tree),--V!=0;);else I!==0?(I!==G&&(X(l,I,l.bl_tree),V--),X(l,A,l.bl_tree),ee(l,V-3,2)):V<=10?(X(l,C,l.bl_tree),ee(l,V-3,3)):(X(l,$,l.bl_tree),ee(l,V-11,7));G=I,Ce=(V=0)===ue?(se=138,3):I===ue?(se=6,3):(se=7,4)}}c(J);var D=!1;function b(l,j,q,U){ee(l,(w<<1)+(U?1:0),3),(function(I,G,ue,V){_e(I),V&&(le(I,ue),le(I,~ue)),h.arraySet(I.pending_buf,I.window,G,ue,I.pending),I.pending+=ue})(l,j,q,!0)}S._tr_init=function(l){D||((function(){var j,q,U,I,G,ue=new Array(m+1);for(I=U=0;I<z-1;I++)for(N[I]=U,j=0;j<1<<T[I];j++)t[U++]=I;for(t[U-1]=I,I=G=0;I<16;I++)for(J[I]=G,j=0;j<1<<H[I];j++)B[G++]=I;for(G>>=7;I<o;I++)for(J[I]=G<<7,j=0;j<1<<H[I]-7;j++)B[256+G++]=I;for(q=0;q<=m;q++)ue[q]=0;for(j=0;j<=143;)ce[2*j+1]=8,j++,ue[8]++;for(;j<=255;)ce[2*j+1]=9,j++,ue[9]++;for(;j<=279;)ce[2*j+1]=7,j++,ue[7]++;for(;j<=287;)ce[2*j+1]=8,j++,ue[8]++;for(Me(ce,_+1,ue),j=0;j<o;j++)y[2*j+1]=5,y[2*j]=Oe(j,5);pe=new be(ce,T,k+1,_,m),Z=new be(y,H,0,o,m),ge=new be(new Array(0),P,0,v,x)})(),D=!0),l.l_desc=new F(l.dyn_ltree,pe),l.d_desc=new F(l.dyn_dtree,Z),l.bl_desc=new F(l.bl_tree,ge),l.bi_buf=0,l.bi_valid=0,ke(l)},S._tr_stored_block=b,S._tr_flush_block=function(l,j,q,U){var I,G,ue=0;0<l.level?(l.strm.data_type===2&&(l.strm.data_type=(function(V){var se,Ce=4093624447;for(se=0;se<=31;se++,Ce>>>=1)if(1&Ce&&V.dyn_ltree[2*se]!==0)return f;if(V.dyn_ltree[18]!==0||V.dyn_ltree[20]!==0||V.dyn_ltree[26]!==0)return r;for(se=32;se<k;se++)if(V.dyn_ltree[2*se]!==0)return r;return f})(l)),$e(l,l.l_desc),$e(l,l.d_desc),ue=(function(V){var se;for(n(V,V.dyn_ltree,V.l_desc.max_code),n(V,V.dyn_dtree,V.d_desc.max_code),$e(V,V.bl_desc),se=v-1;3<=se&&V.bl_tree[2*Y[se]+1]===0;se--);return V.opt_len+=3*(se+1)+5+5+4,se})(l),I=l.opt_len+3+7>>>3,(G=l.static_len+3+7>>>3)<=I&&(I=G)):I=G=q+5,q+4<=I&&j!==-1?b(l,j,q,U):l.strategy===4||G===I?(ee(l,2+(U?1:0),3),qe(l,ce,y)):(ee(l,4+(U?1:0),3),(function(V,se,Ce,Re){var Le;for(ee(V,se-257,5),ee(V,Ce-1,5),ee(V,Re-4,4),Le=0;Le<Re;Le++)ee(V,V.bl_tree[2*Y[Le]+1],3);W(V,V.dyn_ltree,se-1),W(V,V.dyn_dtree,Ce-1)})(l,l.l_desc.max_code+1,l.d_desc.max_code+1,ue+1),qe(l,l.dyn_ltree,l.dyn_dtree)),ke(l),U&&_e(l)},S._tr_tally=function(l,j,q){return l.pending_buf[l.d_buf+2*l.last_lit]=j>>>8&255,l.pending_buf[l.d_buf+2*l.last_lit+1]=255&j,l.pending_buf[l.l_buf+l.last_lit]=255&q,l.last_lit++,j===0?l.dyn_ltree[2*q]++:(l.matches++,j--,l.dyn_ltree[2*(t[q]+k+1)]++,l.dyn_dtree[2*O(j)]++),l.last_lit===l.lit_bufsize-1},S._tr_align=function(l){ee(l,2,3),X(l,E,ce),(function(j){j.bi_valid===16?(le(j,j.bi_buf),j.bi_buf=0,j.bi_valid=0):8<=j.bi_valid&&(j.pending_buf[j.pending++]=255&j.bi_buf,j.bi_buf>>=8,j.bi_valid-=8)})(l)}},{"../utils/common":41}],53:[function(R,M,S){"use strict";M.exports=function(){this.input=null,this.next_in=0,this.avail_in=0,this.total_in=0,this.output=null,this.next_out=0,this.avail_out=0,this.total_out=0,this.msg="",this.state=null,this.data_type=2,this.adler=0}},{}],54:[function(R,M,S){(function(h){(function(f,r){"use strict";if(!f.setImmediate){var c,w,z,k,_=1,o={},v=!1,i=f.document,m=Object.getPrototypeOf&&Object.getPrototypeOf(f);m=m&&m.setTimeout?m:f,c={}.toString.call(f.process)==="[object process]"?function(A){process.nextTick((function(){x(A)}))}:(function(){if(f.postMessage&&!f.importScripts){var A=!0,C=f.onmessage;return f.onmessage=function(){A=!1},f.postMessage("","*"),f.onmessage=C,A}})()?(k="setImmediate$"+Math.random()+"$",f.addEventListener?f.addEventListener("message",E,!1):f.attachEvent("onmessage",E),function(A){f.postMessage(k+A,"*")}):f.MessageChannel?((z=new MessageChannel).port1.onmessage=function(A){x(A.data)},function(A){z.port2.postMessage(A)}):i&&"onreadystatechange"in i.createElement("script")?(w=i.documentElement,function(A){var C=i.createElement("script");C.onreadystatechange=function(){x(A),C.onreadystatechange=null,w.removeChild(C),C=null},w.appendChild(C)}):function(A){setTimeout(x,0,A)},m.setImmediate=function(A){typeof A!="function"&&(A=new Function(""+A));for(var C=new Array(arguments.length-1),$=0;$<C.length;$++)C[$]=arguments[$+1];var T={callback:A,args:C};return o[_]=T,c(_),_++},m.clearImmediate=d}function d(A){delete o[A]}function x(A){if(v)setTimeout(x,0,A);else{var C=o[A];if(C){v=!0;try{(function($){var T=$.callback,H=$.args;switch(H.length){case 0:T();break;case 1:T(H[0]);break;case 2:T(H[0],H[1]);break;case 3:T(H[0],H[1],H[2]);break;default:T.apply(r,H)}})(C)}finally{d(A),v=!1}}}}function E(A){A.source===f&&typeof A.data=="string"&&A.data.indexOf(k)===0&&x(+A.data.slice(k.length))}})(typeof self=="undefined"?h===void 0?this:h:self)}).call(this,ze.g!==void 0?ze.g:typeof self!="undefined"?self:typeof window!="undefined"?window:{})},{}]},{},[10])(10)},899:(te,je,ze)=>{"use strict";te.exports=ze.p+"6600b268dd81380303ff.wasm"},878:(te,je,ze)=>{"use strict";te.exports=ze.p+"a76a7110223ca05112c8.wasm"}},Xu={};function xe(te){var je=Xu[te];if(je!==void 0)return je.exports;var ze=Xu[te]={id:te,loaded:!1,exports:{}};return Yu[te](ze,ze.exports,xe),ze.loaded=!0,ze.exports}xe.m=Yu,xe.n=te=>{var je=te&&te.__esModule?()=>te.default:()=>te;return xe.d(je,{a:je}),je},xe.d=(te,je)=>{for(var ze in je)xe.o(je,ze)&&!xe.o(te,ze)&&Object.defineProperty(te,ze,{enumerable:!0,get:je[ze]})},xe.f={},xe.e=te=>Promise.all(Object.keys(xe.f).reduce(((je,ze)=>(xe.f[ze](te,je),je)),[])),xe.u=te=>"core.ruffle."+{159:"d0be56622fe4dc49e6e5",339:"75932ea73bc1eb7dd589"}[te]+".js",xe.g=(function(){if(typeof globalThis=="object")return globalThis;try{return this||new Function("return this")()}catch(te){if(typeof window=="object")return window}})(),xe.hmd=te=>((te=Object.create(te)).children||(te.children=[]),Object.defineProperty(te,"exports",{enumerable:!0,set:()=>{throw new Error("ES Modules may not assign module.exports or exports.*, Use ESM export syntax, instead: "+te.id)}}),te),xe.o=(te,je)=>Object.prototype.hasOwnProperty.call(te,je),au={},Cu="ruffle-selfhosted:",xe.l=(te,je,ze,R)=>{if(au[te])au[te].push(je);else{var M,S;if(ze!==void 0)for(var h=document.getElementsByTagName("script"),f=0;f<h.length;f++){var r=h[f];if(r.getAttribute("src")==te||r.getAttribute("data-webpack")==Cu+ze){M=r;break}}M||(S=!0,(M=document.createElement("script")).charset="utf-8",M.timeout=120,xe.nc&&M.setAttribute("nonce",xe.nc),M.setAttribute("data-webpack",Cu+ze),M.src=te),au[te]=[je];var c=(z,k)=>{M.onerror=M.onload=null,clearTimeout(w);var _=au[te];if(delete au[te],M.parentNode&&M.parentNode.removeChild(M),_&&_.forEach((o=>o(k))),z)return z(k)},w=setTimeout(c.bind(null,void 0,{type:"timeout",target:M}),12e4);M.onerror=c.bind(null,M.onerror),M.onload=c.bind(null,M.onload),S&&document.head.appendChild(M)}},xe.r=te=>{typeof Symbol!="undefined"&&Symbol.toStringTag&&Object.defineProperty(te,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(te,"__esModule",{value:!0})},xe.p="",(()=>{xe.b=document.baseURI||self.location.href;var te={179:0};xe.f.j=(R,M)=>{var S=xe.o(te,R)?te[R]:void 0;if(S!==0)if(S)M.push(S[2]);else{var h=new Promise(((c,w)=>S=te[R]=[c,w]));M.push(S[2]=h);var f=xe.p+xe.u(R),r=new Error;xe.l(f,(c=>{if(xe.o(te,R)&&((S=te[R])!==0&&(te[R]=void 0),S)){var w=c&&(c.type==="load"?"missing":c.type),z=c&&c.target&&c.target.src;r.message="Loading chunk "+R+` failed.
(`+w+": "+z+")",r.name="ChunkLoadError",r.type=w,r.request=z,S[1](r)}}),"chunk-"+R,R)}};var je=(R,M)=>{var S,h,[f,r,c]=M,w=0;if(f.some((z=>te[z]!==0))){for(S in r)xe.o(r,S)&&(xe.m[S]=r[S]);c&&c(xe)}for(R&&R(M);w<f.length;w++)h=f[w],xe.o(te,h)&&te[h]&&te[h][0](),te[h]=0},ze=self.webpackChunkruffle_selfhosted=self.webpackChunkruffle_selfhosted||[];ze.forEach(je.bind(null,0)),ze.push=je.bind(null,ze.push.bind(ze))})(),(()=>{"use strict";class te{constructor(e,u,a,s,g){this.major=e,this.minor=u,this.patch=a,this.prIdent=s,this.buildIdent=g}static fromSemver(e){const u=e.split("+"),a=u[0].split("-"),s=a[0].split("."),g=parseInt(s[0],10);let L=0,K=0,ne=null,ie=null;return s[1]!==void 0&&(L=parseInt(s[1],10)),s[2]!==void 0&&(K=parseInt(s[2],10)),a[1]!==void 0&&(ne=a[1].split(".")),u[1]!==void 0&&(ie=u[1].split(".")),new te(g,L,K,ne,ie)}isCompatibleWith(e){return this.major!==0&&this.major===e.major||this.major===0&&e.major===0&&this.minor!==0&&this.minor===e.minor||this.major===0&&e.major===0&&this.minor===0&&e.minor===0&&this.patch!==0&&this.patch===e.patch}hasPrecedenceOver(e){if(this.major>e.major)return!0;if(this.major<e.major)return!1;if(this.minor>e.minor)return!0;if(this.minor<e.minor)return!1;if(this.patch>e.patch)return!0;if(this.patch<e.patch)return!1;if(this.prIdent===null&&e.prIdent!==null)return!0;if(this.prIdent!==null&&e.prIdent===null)return!1;if(this.prIdent!==null&&e.prIdent!==null){const u=/^[0-9]*$/;for(let a=0;a<this.prIdent.length&&a<e.prIdent.length;a+=1){const s=u.test(e.prIdent[a]),g=u.test(this.prIdent[a]);if(!g&&s)return!0;if(g&&s){const L=parseInt(this.prIdent[a],10),K=parseInt(e.prIdent[a],10);if(L>K)return!0;if(L<K)return!1}else{if(g&&!s)return!1;if(!g&&!s){if(this.prIdent[a]>e.prIdent[a])return!0;if(this.prIdent[a]<e.prIdent[a])return!1}}}if(this.prIdent.length>e.prIdent.length)return!0;if(this.prIdent.length<e.prIdent.length)return!1}if(this.buildIdent!==null&&e.buildIdent===null)return!0;if(this.buildIdent===null&&e.buildIdent!==null)return!1;if(this.buildIdent!==null&&e.buildIdent!==null){const u=/^[0-9]*$/;for(let a=0;a<this.buildIdent.length&&a<e.buildIdent.length;a+=1){const s=u.test(this.buildIdent[a]),g=u.test(e.buildIdent[a]);if(!s&&g)return!0;if(s&&g){const L=parseInt(this.buildIdent[a],10),K=parseInt(e.buildIdent[a],10);if(L>K)return!0;if(L<K)return!1}else{if(s&&!g)return!1;if(!s&&!g){if(this.buildIdent[a]>e.buildIdent[a])return!0;if(this.buildIdent[a]<e.buildIdent[a])return!1}}}return this.buildIdent.length>e.buildIdent.length}return!1}isEqual(e){return this.major===e.major&&this.minor===e.minor&&this.patch===e.patch}isStableOrCompatiblePrerelease(e){return e.prIdent===null||this.major===e.major&&this.minor===e.minor&&this.patch===e.patch}}class je{constructor(e){this.requirements=e}satisfiedBy(e){for(const u of this.requirements){let a=!0;for(const{comparator:s,version:g}of u)a=a&&g.isStableOrCompatiblePrerelease(e),s===""||s==="="?a=a&&g.isEqual(e):s===">"?a=a&&e.hasPrecedenceOver(g):s===">="?a=a&&(e.hasPrecedenceOver(g)||g.isEqual(e)):s==="<"?a=a&&g.hasPrecedenceOver(e):s==="<="?a=a&&(g.hasPrecedenceOver(e)||g.isEqual(e)):s==="^"&&(a=a&&g.isCompatibleWith(e));if(a)return!0}return!1}static fromRequirementString(e){const u=e.split(" ");let a=[];const s=[];for(const g of u)if(g==="||")a.length>0&&(s.push(a),a=[]);else if(g.length>0){const L=/[0-9]/.exec(g);if(L){const K=g.slice(0,L.index).trim(),ne=te.fromSemver(g.slice(L.index).trim());a.push({comparator:K,version:ne})}}return a.length>0&&s.push(a),new je(s)}}const ze=async()=>WebAssembly.validate(new Uint8Array([0,97,115,109,1,0,0,0,1,4,1,96,0,0,3,2,1,0,5,3,1,0,1,10,14,1,12,0,65,0,65,0,65,0,252,10,0,0,11])),R=async()=>WebAssembly.validate(new Uint8Array([0,97,115,109,1,0,0,0,1,4,1,96,0,0,3,2,1,0,10,7,1,5,0,208,112,26,11])),M=async()=>WebAssembly.validate(new Uint8Array([0,97,115,109,1,0,0,0,1,4,1,96,0,0,3,2,1,0,10,12,1,10,0,67,0,0,0,0,252,0,26,11])),S=async()=>WebAssembly.validate(new Uint8Array([0,97,115,109,1,0,0,0,1,4,1,96,0,0,3,2,1,0,10,8,1,6,0,65,0,192,26,11])),h=async()=>WebAssembly.validate(new Uint8Array([0,97,115,109,1,0,0,0,1,5,1,96,0,1,123,3,2,1,0,10,10,1,8,0,65,0,253,15,253,98,11]));function f(p){const e=typeof Function.prototype.toString=="function"?Function.prototype.toString():null;return typeof e=="string"&&e.indexOf("[native code]")>=0&&Function.prototype.toString.call(p).indexOf("[native code]")>=0}function r(){typeof Array.prototype.reduce=="function"&&f(Array.prototype.reduce)||Object.defineProperty(Array.prototype,"reduce",{value(...p){if(p.length===0&&window.Prototype&&window.Prototype.Version&&window.Prototype.Version<"1.6.1")return this.length>1?this:this[0];const e=p[0];if(this===null)throw new TypeError("Array.prototype.reduce called on null or undefined");if(typeof e!="function")throw new TypeError(`${e} is not a function`);const u=Object(this),a=u.length>>>0;let s,g=0;if(p.length>=2)s=p[1];else{for(;g<a&&!(g in u);)g++;if(g>=a)throw new TypeError("Reduce of empty array with no initial value");s=u[g++]}for(;g<a;)g in u&&(s=e(s,u[g],g,u)),g++;return s}}),typeof Window=="function"&&f(Window)||typeof window.constructor=="function"&&f(window.constructor)&&(window.Window=window.constructor),window.Reflect!==void 0&&window.Reflect!==null||(window.Reflect={}),typeof Reflect.get!="function"&&Object.defineProperty(Reflect,"get",{value:(p,e)=>p[e]}),typeof Reflect.set!="function"&&Object.defineProperty(Reflect,"set",{value(p,e,u){p[e]=u}}),typeof Reflect.has!="function"&&Object.defineProperty(Reflect,"has",{value:(p,e)=>e in p}),typeof Reflect.ownKeys!="function"&&Object.defineProperty(Reflect,"ownKeys",{value:p=>[...Object.getOwnPropertyNames(p),...Object.getOwnPropertySymbols(p)]})}let c=null,w=!1;try{if(document.currentScript!==void 0&&document.currentScript!==null&&"src"in document.currentScript&&document.currentScript.src!==""){let p=document.currentScript.src;p.endsWith(".js")||p.endsWith("/")||(p+="/"),c=new URL(".",p),w=c.protocol.includes("extension")}}catch(p){console.warn("Unable to get currentScript URL")}function z(p){var e;let u=(e=c==null?void 0:c.href)!==null&&e!==void 0?e:"";return!w&&"publicPath"in p&&p.publicPath!==null&&p.publicPath!==void 0&&(u=p.publicPath),u===""||u.endsWith("/")||(u+="/"),u}let k=null;async function _(p,e,u,a){return k===null&&(k=(async function(s,g){var L;r();const K=(await Promise.all([ze(),h(),M(),S(),R()])).every(Boolean);K||console.log("Some WebAssembly extensions are NOT available, falling back to the vanilla WebAssembly module"),xe.p=z(s);const{default:ne,Ruffle:ie}=await(K?xe.e(339).then(xe.bind(xe,339)):xe.e(159).then(xe.bind(xe,159)));let oe;const me=K?new URL(xe(899),xe.b):new URL(xe(878),xe.b),he=await fetch(me);if(g&&typeof ReadableStream=="function"){const de=((L=he==null?void 0:he.headers)===null||L===void 0?void 0:L.get("content-length"))||"";let Se=0;const Ve=parseInt(de);oe=new Response(new ReadableStream({async start(Be){var Te;const Ne=(Te=he.body)===null||Te===void 0?void 0:Te.getReader();if(!Ne)throw"Response had no body";for(g(Se,Ve);;){const{done:nu,value:Q}=await Ne.read();if(nu)break;Q!=null&&Q.byteLength&&(Se+=Q==null?void 0:Q.byteLength),Be.enqueue(Q),g(Se,Ve)}Be.close()}}),he)}else oe=he;return await ne(oe),ie})(u,a)),new(await k)(p,e,u)}class o{constructor(e){this.value=e}valueOf(){return this.value}}class v extends o{constructor(e="???"){super(e)}toString(e){return`{${this.value}}`}}class i extends o{constructor(e,u={}){super(e),this.opts=u}toString(e){try{return e.memoizeIntlObject(Intl.NumberFormat,this.opts).format(this.value)}catch(u){return e.reportError(u),this.value.toString(10)}}}class m extends o{constructor(e,u={}){super(e),this.opts=u}toString(e){try{return e.memoizeIntlObject(Intl.DateTimeFormat,this.opts).format(this.value)}catch(u){return e.reportError(u),new Date(this.value).toISOString()}}}const d=100,x="\u2068",E="\u2069";function A(p,e,u){return u===e||u instanceof i&&e instanceof i&&u.value===e.value||e instanceof i&&typeof u=="string"&&u===p.memoizeIntlObject(Intl.PluralRules,e.opts).select(e.value)}function C(p,e,u){return e[u]?P(p,e[u].value):(p.reportError(new RangeError("No default")),new v)}function $(p,e){const u=[],a=Object.create(null);for(const s of e)s.type==="narg"?a[s.name]=T(p,s.value):u.push(T(p,s));return{positional:u,named:a}}function T(p,e){switch(e.type){case"str":return e.value;case"num":return new i(e.value,{minimumFractionDigits:e.precision});case"var":return(function(u,{name:a}){let s;if(u.params){if(!Object.prototype.hasOwnProperty.call(u.params,a))return new v(`$${a}`);s=u.params[a]}else{if(!u.args||!Object.prototype.hasOwnProperty.call(u.args,a))return u.reportError(new ReferenceError(`Unknown variable: $${a}`)),new v(`$${a}`);s=u.args[a]}if(s instanceof o)return s;switch(typeof s){case"string":return s;case"number":return new i(s);case"object":if(s instanceof Date)return new m(s.getTime());default:return u.reportError(new TypeError(`Variable type not supported: $${a}, ${typeof s}`)),new v(`$${a}`)}})(p,e);case"mesg":return(function(u,{name:a,attr:s}){const g=u.bundle._messages.get(a);if(!g)return u.reportError(new ReferenceError(`Unknown message: ${a}`)),new v(a);if(s){const L=g.attributes[s];return L?P(u,L):(u.reportError(new ReferenceError(`Unknown attribute: ${s}`)),new v(`${a}.${s}`))}return g.value?P(u,g.value):(u.reportError(new ReferenceError(`No value: ${a}`)),new v(a))})(p,e);case"term":return(function(u,{name:a,attr:s,args:g}){const L=`-${a}`,K=u.bundle._terms.get(L);if(!K)return u.reportError(new ReferenceError(`Unknown term: ${L}`)),new v(L);if(s){const ie=K.attributes[s];if(ie){u.params=$(u,g).named;const oe=P(u,ie);return u.params=null,oe}return u.reportError(new ReferenceError(`Unknown attribute: ${s}`)),new v(`${L}.${s}`)}u.params=$(u,g).named;const ne=P(u,K.value);return u.params=null,ne})(p,e);case"func":return(function(u,{name:a,args:s}){let g=u.bundle._functions[a];if(!g)return u.reportError(new ReferenceError(`Unknown function: ${a}()`)),new v(`${a}()`);if(typeof g!="function")return u.reportError(new TypeError(`Function ${a}() is not callable`)),new v(`${a}()`);try{let L=$(u,s);return g(L.positional,L.named)}catch(L){return u.reportError(L),new v(`${a}()`)}})(p,e);case"select":return(function(u,{selector:a,variants:s,star:g}){let L=T(u,a);if(L instanceof v)return C(u,s,g);for(const K of s)if(A(u,L,T(u,K.key)))return P(u,K.value);return C(u,s,g)})(p,e);default:return new v}}function H(p,e){if(p.dirty.has(e))return p.reportError(new RangeError("Cyclic reference")),new v;p.dirty.add(e);const u=[],a=p.bundle._useIsolating&&e.length>1;for(const s of e)if(typeof s!="string"){if(p.placeables++,p.placeables>d)throw p.dirty.delete(e),new RangeError(`Too many placeables expanded: ${p.placeables}, max allowed is ${d}`);a&&u.push(x),u.push(T(p,s).toString(p)),a&&u.push(E)}else u.push(p.bundle._transform(s));return p.dirty.delete(e),u.join("")}function P(p,e){return typeof e=="string"?p.bundle._transform(e):H(p,e)}class Y{constructor(e,u,a){this.dirty=new WeakSet,this.params=null,this.placeables=0,this.bundle=e,this.errors=u,this.args=a}reportError(e){if(!(this.errors&&e instanceof Error))throw e;this.errors.push(e)}memoizeIntlObject(e,u){let a=this.bundle._intls.get(e);a||(a={},this.bundle._intls.set(e,a));let s=JSON.stringify(u);return a[s]||(a[s]=new e(this.bundle.locales,u)),a[s]}}function ce(p,e){const u=Object.create(null);for(const[a,s]of Object.entries(p))e.includes(a)&&(u[a]=s.valueOf());return u}const y=["unitDisplay","currencyDisplay","useGrouping","minimumIntegerDigits","minimumFractionDigits","maximumFractionDigits","minimumSignificantDigits","maximumSignificantDigits"];function B(p,e){let u=p[0];if(u instanceof v)return new v(`NUMBER(${u.valueOf()})`);if(u instanceof i)return new i(u.valueOf(),{...u.opts,...ce(e,y)});if(u instanceof m)return new i(u.valueOf(),{...ce(e,y)});throw new TypeError("Invalid argument to NUMBER")}const t=["dateStyle","timeStyle","fractionalSecondDigits","dayPeriod","hour12","weekday","era","year","month","day","hour","minute","second","timeZoneName"];function N(p,e){let u=p[0];if(u instanceof v)return new v(`DATETIME(${u.valueOf()})`);if(u instanceof m)return new m(u.valueOf(),{...u.opts,...ce(e,t)});if(u instanceof i)return new m(u.valueOf(),{...ce(e,t)});throw new TypeError("Invalid argument to DATETIME")}const pe=new Map;class Z{constructor(e,{functions:u,useIsolating:a=!0,transform:s=(g=>g)}={}){this._terms=new Map,this._messages=new Map,this.locales=Array.isArray(e)?e:[e],this._functions={NUMBER:B,DATETIME:N,...u},this._useIsolating=a,this._transform=s,this._intls=(function(g){const L=Array.isArray(g)?g.join(" "):g;let K=pe.get(L);return K===void 0&&(K=new Map,pe.set(L,K)),K})(e)}hasMessage(e){return this._messages.has(e)}getMessage(e){return this._messages.get(e)}addResource(e,{allowOverrides:u=!1}={}){const a=[];for(let s=0;s<e.body.length;s++){let g=e.body[s];if(g.id.startsWith("-")){if(u===!1&&this._terms.has(g.id)){a.push(new Error(`Attempt to override an existing term: "${g.id}"`));continue}this._terms.set(g.id,g)}else{if(u===!1&&this._messages.has(g.id)){a.push(new Error(`Attempt to override an existing message: "${g.id}"`));continue}this._messages.set(g.id,g)}}return a}formatPattern(e,u=null,a=null){if(typeof e=="string")return this._transform(e);let s=new Y(this,a,u);try{return H(s,e).toString(s)}catch(g){if(s.errors&&g instanceof Error)return s.errors.push(g),new v().toString(s);throw g}}}const ge=/^(-?[a-zA-Z][\w-]*) *= */gm,J=/\.([a-zA-Z][\w-]*) *= */y,be=/\*?\[/y,F=/(-?[0-9]+(?:\.([0-9]+))?)/y,O=/([a-zA-Z][\w-]*)/y,le=/([$-])?([a-zA-Z][\w-]*)(?:\.([a-zA-Z][\w-]*))?/y,ee=/^[A-Z][A-Z0-9_-]*$/,X=/([^{}\n\r]+)/y,Oe=/([^\\"\n\r]*)/y,Me=/\\([\\"])/y,ke=/\\u([a-fA-F0-9]{4})|\\U([a-fA-F0-9]{6})/y,_e=/^\n+/,Pe=/ +$/,Fe=/ *\r?\n/g,qe=/( *)$/,$e=/{\s*/y,n=/\s*}/y,W=/\[\s*/y,D=/\s*] */y,b=/\s*\(\s*/y,l=/\s*->\s*/y,j=/\s*:\s*/y,q=/\s*,?\s*/y,U=/\s+/y;class I{constructor(e){this.body=[],ge.lastIndex=0;let u=0;for(;;){let Q=ge.exec(e);if(Q===null)break;u=ge.lastIndex;try{this.body.push(ne(Q[1]))}catch(ve){if(ve instanceof SyntaxError)continue;throw ve}}function a(Q){return Q.lastIndex=u,Q.test(e)}function s(Q,ve){if(e[u]===Q)return u++,!0;if(ve)throw new ve(`Expected ${Q}`);return!1}function g(Q,ve){if(a(Q))return u=Q.lastIndex,!0;if(ve)throw new ve(`Expected ${Q.toString()}`);return!1}function L(Q){Q.lastIndex=u;let ve=Q.exec(e);if(ve===null)throw new SyntaxError(`Expected ${Q.toString()}`);return u=Q.lastIndex,ve}function K(Q){return L(Q)[1]}function ne(Q){let ve=ie(),De=(function(){let Ke=Object.create(null);for(;a(J);){let Ge=K(J),We=ie();if(We===null)throw new SyntaxError("Expected attribute value");Ke[Ge]=We}return Ke})();if(ve===null&&Object.keys(De).length===0)throw new SyntaxError("Expected message value or attributes");return{id:Q,value:ve,attributes:De}}function ie(){let Q;if(a(X)&&(Q=K(X)),e[u]==="{"||e[u]==="}")return oe(Q?[Q]:[],1/0);let ve=Te();return ve?Q?oe([Q,ve],ve.length):(ve.value=Ne(ve.value,_e),oe([ve],ve.length)):Q?Ne(Q,Pe):null}function oe(Q=[],ve){for(;;){if(a(X)){Q.push(K(X));continue}if(e[u]==="{"){Q.push(me());continue}if(e[u]==="}")throw new SyntaxError("Unbalanced closing brace");let We=Te();if(!We)break;Q.push(We),ve=Math.min(ve,We.length)}let De=Q.length-1,Ke=Q[De];typeof Ke=="string"&&(Q[De]=Ne(Ke,Pe));let Ge=[];for(let We of Q)We instanceof G&&(We=We.value.slice(0,We.value.length-ve)),We&&Ge.push(We);return Ge}function me(){g($e,SyntaxError);let Q=he();if(g(n))return Q;if(g(l)){let ve=(function(){let De,Ke=[],Ge=0;for(;a(be);){s("*")&&(De=Ge);let We=de(),On=ie();if(On===null)throw new SyntaxError("Expected variant value");Ke[Ge++]={key:We,value:On}}if(Ge===0)return null;if(De===void 0)throw new SyntaxError("Expected default variant");return{variants:Ke,star:De}})();return g(n,SyntaxError),{type:"select",selector:Q,...ve}}throw new SyntaxError("Unclosed placeable")}function he(){if(e[u]==="{")return me();if(a(le)){let[,Q,ve,De=null]=L(le);if(Q==="$")return{type:"var",name:ve};if(g(b)){let Ke=(function(){let Ge=[];for(;;){switch(e[u]){case")":return u++,Ge;case void 0:throw new SyntaxError("Unclosed argument list")}Ge.push(fe()),g(q)}})();if(Q==="-")return{type:"term",name:ve,attr:De,args:Ke};if(ee.test(ve))return{type:"func",name:ve,args:Ke};throw new SyntaxError("Function names must be all upper-case")}return Q==="-"?{type:"term",name:ve,attr:De,args:[]}:{type:"mesg",name:ve,attr:De}}return Se()}function fe(){let Q=he();return Q.type!=="mesg"?Q:g(j)?{type:"narg",name:Q.name,value:Se()}:Q}function de(){let Q;return g(W,SyntaxError),Q=a(F)?Ve():{type:"str",value:K(O)},g(D,SyntaxError),Q}function Se(){if(a(F))return Ve();if(e[u]==='"')return(function(){s('"',SyntaxError);let Q="";for(;;){if(Q+=K(Oe),e[u]!=="\\"){if(s('"'))return{type:"str",value:Q};throw new SyntaxError("Unclosed string literal")}Q+=Be()}})();throw new SyntaxError("Invalid expression")}function Ve(){let[,Q,ve=""]=L(F),De=ve.length;return{type:"num",value:parseFloat(Q),precision:De}}function Be(){if(a(Me))return K(Me);if(a(ke)){let[,Q,ve]=L(ke),De=parseInt(Q||ve,16);return De<=55295||57344<=De?String.fromCodePoint(De):"\uFFFD"}throw new SyntaxError("Unknown escape sequence")}function Te(){let Q=u;switch(g(U),e[u]){case".":case"[":case"*":case"}":case void 0:return!1;case"{":return nu(e.slice(Q,u))}return e[u-1]===" "&&nu(e.slice(Q,u))}function Ne(Q,ve){return Q.replace(ve,"")}function nu(Q){let ve=Q.replace(Fe,`
`),De=qe.exec(Q)[1].length;return new G(ve,De)}}}class G{constructor(e,u){this.value=e,this.length=u}}const ue=new RegExp("^([a-z]{2,3}|\\*)(?:-([a-z]{4}|\\*))?(?:-([a-z]{2}|\\*))?(?:-(([0-9][a-z0-9]{3}|[a-z0-9]{5,8})|\\*))?$","i");class V{constructor(e){const u=ue.exec(e.replace(/_/g,"-"));if(!u)return void(this.isWellFormed=!1);let[,a,s,g,L]=u;a&&(this.language=a.toLowerCase()),s&&(this.script=s[0].toUpperCase()+s.slice(1)),g&&(this.region=g.toUpperCase()),this.variant=L,this.isWellFormed=!0}isEqual(e){return this.language===e.language&&this.script===e.script&&this.region===e.region&&this.variant===e.variant}matches(e,u=!1,a=!1){return(this.language===e.language||u&&this.language===void 0||a&&e.language===void 0)&&(this.script===e.script||u&&this.script===void 0||a&&e.script===void 0)&&(this.region===e.region||u&&this.region===void 0||a&&e.region===void 0)&&(this.variant===e.variant||u&&this.variant===void 0||a&&e.variant===void 0)}toString(){return[this.language,this.script,this.region,this.variant].filter((e=>e!==void 0)).join("-")}clearVariants(){this.variant=void 0}clearRegion(){this.region=void 0}addLikelySubtags(){const e=(function(u){if(Object.prototype.hasOwnProperty.call(se,u))return new V(se[u]);const a=new V(u);return a.language&&Ce.includes(a.language)?(a.region=a.language.toUpperCase(),a):null})(this.toString().toLowerCase());return!!e&&(this.language=e.language,this.script=e.script,this.region=e.region,this.variant=e.variant,!0)}}const se={ar:"ar-arab-eg","az-arab":"az-arab-ir","az-ir":"az-arab-ir",be:"be-cyrl-by",da:"da-latn-dk",el:"el-grek-gr",en:"en-latn-us",fa:"fa-arab-ir",ja:"ja-jpan-jp",ko:"ko-kore-kr",pt:"pt-latn-br",sr:"sr-cyrl-rs","sr-ru":"sr-latn-ru",sv:"sv-latn-se",ta:"ta-taml-in",uk:"uk-cyrl-ua",zh:"zh-hans-cn","zh-hant":"zh-hant-tw","zh-hk":"zh-hant-hk","zh-mo":"zh-hant-mo","zh-tw":"zh-hant-tw","zh-gb":"zh-hant-gb","zh-us":"zh-hant-us"},Ce=["az","bg","cs","de","es","fi","fr","hu","it","lt","lv","nl","pl","ro","ru"];function Re(p,e,{strategy:u="filtering",defaultLocale:a}={}){const s=(function(g,L,K){const ne=new Set,ie=new Map;for(let oe of L)new V(oe).isWellFormed&&ie.set(oe,new V(oe));e:for(const oe of g){const me=oe.toLowerCase(),he=new V(me);if(he.language!==void 0){for(const fe of ie.keys())if(me===fe.toLowerCase()){if(ne.add(fe),ie.delete(fe),K==="lookup")return Array.from(ne);if(K==="filtering")continue;continue e}for(const[fe,de]of ie.entries())if(de.matches(he,!0,!1)){if(ne.add(fe),ie.delete(fe),K==="lookup")return Array.from(ne);if(K==="filtering")continue;continue e}if(he.addLikelySubtags()){for(const[fe,de]of ie.entries())if(de.matches(he,!0,!1)){if(ne.add(fe),ie.delete(fe),K==="lookup")return Array.from(ne);if(K==="filtering")continue;continue e}}he.clearVariants();for(const[fe,de]of ie.entries())if(de.matches(he,!0,!0)){if(ne.add(fe),ie.delete(fe),K==="lookup")return Array.from(ne);if(K==="filtering")continue;continue e}if(he.clearRegion(),he.addLikelySubtags()){for(const[fe,de]of ie.entries())if(de.matches(he,!0,!1)){if(ne.add(fe),ie.delete(fe),K==="lookup")return Array.from(ne);if(K==="filtering")continue;continue e}}he.clearRegion();for(const[fe,de]of ie.entries())if(de.matches(he,!0,!0)){if(ne.add(fe),ie.delete(fe),K==="lookup")return Array.from(ne);if(K==="filtering")continue;continue e}}}return Array.from(ne)})(Array.from(p!=null?p:[]).map(String),Array.from(e!=null?e:[]).map(String),u);if(u==="lookup"){if(a===void 0)throw new Error("defaultLocale cannot be undefined for strategy `lookup`");s.length===0&&s.push(a)}else a&&!s.includes(a)&&s.push(a);return s}const Le={"ar-SA":{"context_menu.ftl":`context-menu-download-swf = \u062A\u062D\u0645\u064A\u0644 .swf
context-menu-copy-debug-info = \u0646\u0633\u062E \u0645\u0639\u0644\u0648\u0645\u0627\u062A \u0627\u0644\u062A\u0635\u062D\u064A\u062D
context-menu-open-save-manager = \u0641\u062A\u062D \u0645\u062F\u064A\u0631 \u0627\u0644\u062D\u0641\u0638
context-menu-about-ruffle =
    { $flavor ->
        [extension] \u062D\u0648\u0644 \u0645\u0644\u062D\u0642 \u0631\u0641\u0644 ({ $version })
       *[other] \u062D\u0648\u0644 \u0631\u0641\u0644 ({ $version })
    }
context-menu-hide = \u0625\u062E\u0641\u0627\u0621 \u0647\u0630\u0647 \u0627\u0644\u0642\u0627\u0626\u0645\u0629
context-menu-exit-fullscreen = \u0627\u0644\u062E\u0631\u0648\u062C \u0645\u0646 \u0648\u0636\u0639\u064A\u0629 \u0627\u0644\u0634\u0627\u0634\u0629 \u0627\u0644\u0643\u0627\u0645\u0644\u0629
context-menu-enter-fullscreen = \u062A\u0641\u0639\u064A\u0644 \u0648\u0636\u0639\u064A\u0629 \u0627\u0644\u0634\u0627\u0634\u0629 \u0627\u0644\u0643\u0627\u0645\u0644\u0629
context-menu-volume-controls = \u0627\u0644\u062A\u062D\u0643\u0645 \u0628\u0627\u0644\u0635\u0648\u062A
`,"messages.ftl":`message-cant-embed =
    \u0644\u0645 \u062A\u0643\u0646 \u0631\u0641\u0644 \u0642\u0627\u062F\u0631\u0629 \u0639\u0644\u0649 \u062A\u0634\u063A\u064A\u0644 \u0627\u0644\u0641\u0644\u0627\u0634 \u0627\u0644\u0645\u0636\u0645\u0646\u0629 \u0641\u064A \u0647\u0630\u0647 \u0627\u0644\u0635\u0641\u062D\u0629.
    \u064A\u0645\u0643\u0646\u0643 \u0645\u062D\u0627\u0648\u0644\u0629 \u0641\u062A\u062D \u0627\u0644\u0645\u0644\u0641 \u0641\u064A \u0639\u0644\u0627\u0645\u0629 \u062A\u0628\u0648\u064A\u0628 \u0645\u0646\u0641\u0635\u0644\u0629\u060C \u0644\u062A\u062C\u0627\u0648\u0632 \u0647\u0630\u0647 \u0627\u0644\u0645\u0634\u0643\u0644\u0629.
panic-title = \u0644\u0642\u062F \u062D\u062F\u062B \u062E\u0637\u0623 \u0645\u0627 :(
more-info = \u0645\u0639\u0644\u0648\u0645\u0627\u062A \u0623\u0643\u062B\u0631
run-anyway = \u0627\u0644\u062A\u0634\u063A\u064A\u0644 \u0639\u0644\u0649 \u0623\u064A \u062D\u0627\u0644
continue = \u0627\u0644\u0627\u0633\u062A\u0645\u0631\u0627\u0631
report-bug = \u0625\u0628\u0644\u0627\u063A \u0639\u0646 \u062E\u0644\u0644
update-ruffle = \u062A\u062D\u062F\u064A\u062B \u0631\u0641\u0644
ruffle-demo = \u0648\u064A\u0628 \u0627\u0644\u062A\u062C\u0631\u064A\u0628\u064A
ruffle-desktop = \u0628\u0631\u0646\u0627\u0645\u062C \u0633\u0637\u062D \u0627\u0644\u0645\u0643\u062A\u0628
ruffle-wiki = \u0639\u0631\u0636 \u0631\u0641\u0644 \u0648\u064A\u0643\u064A
enable-hardware-acceleration = \u064A\u0628\u062F\u0648 \u0623\u0646 \u062A\u0633\u0627\u0631\u0639 \u0627\u0644\u0623\u062C\u0647\u0632\u0629 \u063A\u064A\u0631 \u0645\u0641\u0639\u0644. \u0628\u064A\u0646\u0645\u0627 \u0642\u062F \u064A\u0639\u0645\u0644 \u0627\u0644\u0631\u0648\u0641\u0644\u060C \u0642\u062F \u064A\u0643\u0648\u0646 \u0628\u0637\u064A\u0626\u0627\u064B \u0628\u0634\u0643\u0644 \u063A\u064A\u0631 \u0645\u0639\u0642\u0648\u0644. \u064A\u0645\u0643\u0646\u0643 \u0645\u0639\u0631\u0641\u0629 \u0643\u064A\u0641\u064A\u0629 \u062A\u0645\u0643\u064A\u0646 \u062A\u0633\u0627\u0631\u0639 \u0627\u0644\u0623\u062C\u0647\u0632\u0629 \u0645\u0646 \u062E\u0644\u0627\u0644 \u0645\u062A\u0627\u0628\u0639\u0629 \u0647\u0630\u0627 \u0627\u0644\u0631\u0627\u0628\u0637.
view-error-details = \u0639\u0631\u0636 \u062A\u0641\u0627\u0635\u064A\u0644 \u0627\u0644\u062E\u0637\u0623
open-in-new-tab = \u0641\u062A\u062D \u0641\u064A \u0639\u0644\u0627\u0645\u0629 \u062A\u0628\u0648\u064A\u0628 \u062C\u062F\u064A\u062F\u0629
click-to-unmute = \u0627\u0646\u0642\u0631 \u0644\u0625\u0644\u063A\u0627\u0621 \u0627\u0644\u0643\u062A\u0645
error-file-protocol =
    \u064A\u0628\u062F\u0648 \u0623\u0646\u0643 \u062A\u0642\u0648\u0645 \u0628\u062A\u0634\u063A\u064A\u0644 \u0631\u0641\u0644 \u0639\u0644\u0649 \u0628\u0631\u0648\u062A\u0648\u0643\u0648\u0644 "\u0627\u0644\u0645\u0644\u0641:".
    \u0647\u0630\u0627 \u0644\u0646 \u064A\u0639\u0645\u0644 \u0644\u0623\u0646 \u0627\u0644\u0645\u062A\u0635\u0641\u062D\u0627\u062A \u062A\u0645\u0646\u0639 \u0627\u0644\u0639\u062F\u064A\u062F \u0645\u0646 \u0627\u0644\u0645\u064A\u0632\u0627\u062A \u0645\u0646 \u0627\u0644\u0639\u0645\u0644 \u0644\u0623\u0633\u0628\u0627\u0628 \u0623\u0645\u0646\u064A\u0629.
    \u0628\u062F\u0644\u0627\u064B \u0645\u0646 \u0630\u0644\u0643\u060C \u0646\u062F\u0639\u0648\u0643 \u0625\u0644\u0649 \u0625\u0639\u062F\u0627\u062F \u062E\u0627\u062F\u0645 \u0645\u062D\u0644\u064A \u0623\u0648 \u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0639\u0631\u0636 \u0627\u0644\u0648\u064A\u0628 \u0623\u0648 \u062A\u0637\u0628\u064A\u0642 \u0633\u0637\u062D \u0627\u0644\u0645\u0643\u062A\u0628.
error-javascript-config =
    \u062A\u0639\u0631\u0636 \u0631\u0641\u0644 \u0625\u0644\u0649 \u0645\u0634\u0643\u0644\u0629 \u0631\u0626\u064A\u0633\u064A\u0629 \u0628\u0633\u0628\u0628 \u0627\u0644\u0625\u0639\u062F\u0627\u062F\u0627\u062A \u0627\u0644\u062E\u0627\u0637\u0626\u0629 \u0644\u0644\u062C\u0627\u0641\u0627 \u0633\u0643\u0631\u064A\u0628\u062A.
    \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u0646\u062D\u0646 \u0646\u062F\u0639\u0648\u0643 \u0625\u0644\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u062A\u0641\u0627\u0635\u064A\u0644 \u0627\u0644\u062E\u0637\u0623 \u0644\u0645\u0639\u0631\u0641\u0629 \u0633\u0628\u0628 \u0627\u0644\u0645\u0634\u0643\u0644\u0629.
    \u064A\u0645\u0643\u0646\u0643 \u0623\u064A\u0636\u0627 \u0627\u0644\u0631\u062C\u0648\u0639 \u0625\u0644\u0649 \u0631\u0641\u0644 \u0648\u064A\u0643\u064A \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u0645\u0633\u0627\u0639\u062F\u0629.
error-wasm-not-found =
    \u0641\u0634\u0644 \u0631\u0641\u0644 \u0641\u064A \u062A\u062D\u0645\u064A\u0644 \u0645\u0643\u0648\u0646 \u0627\u0644\u0645\u0644\u0641 ".wasm" \u0627\u0644\u0645\u0637\u0644\u0648\u0628.
    \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u0627\u0644\u0631\u062C\u0627\u0621 \u0627\u0644\u062A\u0623\u0643\u062F \u0645\u0646 \u0623\u0646 \u0627\u0644\u0645\u0644\u0641 \u0642\u062F \u062A\u0645 \u062A\u062D\u0645\u064A\u0644\u0647 \u0628\u0634\u0643\u0644 \u0635\u062D\u064A\u062D.
    \u0625\u0630\u0627 \u0627\u0633\u062A\u0645\u0631\u062A \u0627\u0644\u0645\u0634\u0643\u0644\u0629\u060C \u0642\u062F \u062A\u062D\u062A\u0627\u062C \u0625\u0644\u0649 \u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0625\u0639\u062F\u0627\u062F\u0627\u062A "\u0627\u0644\u0645\u0633\u0627\u0631 \u0627\u0644\u0639\u0627\u0645": \u0627\u0644\u0631\u062C\u0627\u0621 \u0645\u0631\u0627\u062C\u0639\u0629 \u0631\u0641\u0644 \u0648\u064A\u0643\u064A \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u0645\u0633\u0627\u0639\u062F\u0629.
error-wasm-mime-type =
    \u0648\u0627\u062C\u0647\u062A \u0631\u0641\u0644 \u0645\u0634\u0643\u0644\u0629 \u0631\u0626\u064A\u0633\u064A\u0629 \u0623\u062B\u0646\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u0629 \u0627\u0644\u062A\u0647\u064A\u0626\u0629.
    \u062E\u0627\u062F\u0645 \u0627\u0644\u0648\u064A\u0628 \u0647\u0630\u0627 \u0644\u0627 \u064A\u062E\u062F\u0645 \u0645\u0644\u0641\u0627\u062A ". wasm" \u0645\u0639 \u0646\u0648\u0639 MIME \u0627\u0644\u0635\u062D\u064A\u062D.
    \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u064A\u0631\u062C\u0649 \u0645\u0631\u0627\u062C\u0639\u0629 \u0631\u0641\u0644 \u0648\u064A\u0643\u064A \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u0645\u0633\u0627\u0639\u062F\u0629.
error-swf-fetch =
    \u0641\u0634\u0644 \u0631\u0641\u0644 \u0641\u064A \u062A\u062D\u0645\u064A\u0644 \u0645\u0644\u0641 \u0641\u0644\u0627\u0634 SWF.
    \u0627\u0644\u0633\u0628\u0628 \u0627\u0644\u0623\u0643\u062B\u0631 \u0627\u062D\u062A\u0645\u0627\u0644\u0627 \u0647\u0648 \u0623\u0646 \u0627\u0644\u0645\u0644\u0641 \u0644\u0645 \u064A\u0639\u062F \u0645\u0648\u062C\u0648\u062F\u0627\u060C \u0644\u0630\u0644\u0643 \u0644\u0627 \u064A\u0648\u062C\u062F \u0634\u064A\u0621 \u0644\u064A\u062D\u0645\u0644\u0647 \u0631\u0641\u0644.
    \u062D\u0627\u0648\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u0645\u0648\u0642\u0639 \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u0645\u0633\u0627\u0639\u062F\u0629.
error-swf-cors =
    \u0641\u0634\u0644 \u0627\u0644\u0631\u0648\u0641\u0644 \u0641\u064A \u062A\u062D\u0645\u064A\u0644 \u0645\u0644\u0641 \u0641\u0644\u0627\u0634 SWF.
    \u0645\u0646 \u0627\u0644\u0645\u062D\u062A\u0645\u0644 \u0623\u0646 \u062A\u0645 \u062D\u0638\u0631 \u0627\u0644\u0648\u0635\u0648\u0644 \u0625\u0644\u0649 \u0627\u0644\u0645\u0646\u0627\u0644 \u0628\u0648\u0627\u0633\u0637\u0629 \u0633\u064A\u0627\u0633\u0629 CORS.
    \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u064A\u0631\u062C\u0649 \u0645\u0631\u0627\u062C\u0639\u0629 \u0631\u0641\u0644 \u0648\u064A\u0643\u064A \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u0645\u0633\u0627\u0639\u062F\u0629.
error-wasm-cors =
    \u0641\u0634\u0644 \u0631\u0641\u0644 \u0641\u064A \u062A\u062D\u0645\u064A\u0644 \u0645\u0643\u0648\u0646 \u0645\u0644\u0641 ".wasm" \u0627\u0644\u0645\u0637\u0644\u0648\u0628.
    \u0645\u0646 \u0627\u0644\u0645\u062D\u062A\u0645\u0644 \u0623\u0646 \u062A\u0645 \u062D\u0638\u0631 \u0627\u0644\u0648\u0635\u0648\u0644 \u0625\u0644\u0649 \u0627\u0644\u0645\u0646\u0627\u0644 \u0628\u0648\u0627\u0633\u0637\u0629 \u0633\u064A\u0627\u0633\u0629 CORS.
    \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u064A\u0631\u062C\u0649 \u0645\u0631\u0627\u062C\u0639\u0629 \u0631\u0641\u0644 \u0648\u064A\u0643\u064A \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u0645\u0633\u0627\u0639\u062F\u0629.
error-wasm-invalid =
    \u0648\u0627\u062C\u0647\u062A \u0631\u0641\u0644 \u0645\u0634\u0643\u0644\u0629 \u0631\u0626\u064A\u0633\u064A\u0629 \u0623\u062B\u0646\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u0629 \u0627\u0644\u062A\u0647\u064A\u0626\u0629.
    \u062E\u0627\u062F\u0645 \u0627\u0644\u0648\u064A\u0628 \u0647\u0630\u0627 \u0644\u0627 \u064A\u062E\u062F\u0645 \u0645\u0644\u0641\u0627\u062A ". wasm" \u0645\u0639 \u0646\u0648\u0639 MIME \u0627\u0644\u0635\u062D\u064A\u062D.
    \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u064A\u0631\u062C\u0649 \u0645\u0631\u0627\u062C\u0639\u0629 \u0631\u0641\u0644 \u0648\u064A\u0643\u064A \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u0645\u0633\u0627\u0639\u062F\u0629.
error-wasm-download =
    \u0648\u0627\u062C\u0647\u062A \u0631\u0641\u0644 \u0645\u0634\u0643\u0644\u0629 \u0631\u0626\u064A\u0633\u064A\u0629 \u0623\u062B\u0646\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u062A\u0647\u0627 \u0627\u0644\u062A\u0647\u064A\u0626\u0629.
    \u0647\u0630\u0627 \u064A\u0645\u0643\u0646 \u0623\u0646 \u064A\u062D\u0644 \u0646\u0641\u0633\u0647 \u0641\u064A \u0643\u062B\u064A\u0631 \u0645\u0646 \u0627\u0644\u0623\u062D\u064A\u0627\u0646\u060C \u0644\u0630\u0644\u0643 \u064A\u0645\u0643\u0646\u0643 \u0645\u062D\u0627\u0648\u0644\u0629 \u0625\u0639\u0627\u062F\u0629 \u062A\u062D\u0645\u064A\u0644 \u0627\u0644\u0635\u0641\u062D\u0629.
    \u062E\u0644\u0627\u0641 \u0630\u0644\u0643\u060C \u064A\u0631\u062C\u0649 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0645\u062F\u064A\u0631 \u0627\u0644\u0645\u0648\u0642\u0639.
error-wasm-disabled-on-edge =
    \u0641\u0634\u0644 \u0631\u0641\u0644 \u0641\u064A \u062A\u062D\u0645\u064A\u0644 \u0645\u0643\u0648\u0646 \u0627\u0644\u0645\u0644\u0641 ".wasm" \u0627\u0644\u0645\u0637\u0644\u0648\u0628.
    \u0644\u0625\u0635\u0644\u0627\u062D \u0647\u0630\u0647 \u0627\u0644\u0645\u0634\u0643\u0644\u0629\u060C \u062D\u0627\u0648\u0644 \u0641\u062A\u062D \u0625\u0639\u062F\u0627\u062F\u0627\u062A \u0627\u0644\u0645\u062A\u0635\u0641\u062D \u0627\u0644\u062E\u0627\u0635 \u0628\u0643\u060C \u0627\u0646\u0642\u0631 \u0641\u0648\u0642 "\u0627\u0644\u062E\u0635\u0648\u0635\u064A\u0629\u060C \u0627\u0644\u0628\u062D\u062B\u060C \u0627\u0644\u062E\u062F\u0645\u0627\u062A"\u060C \u0648\u0627\u0644\u062A\u0645\u0631\u064A\u0631 \u0644\u0623\u0633\u0641\u0644\u060C \u0648\u0625\u064A\u0642\u0627\u0641 "\u062A\u0639\u0632\u064A\u0632 \u0623\u0645\u0627\u0646\u0643 \u0639\u0644\u0649 \u0627\u0644\u0648\u064A\u0628".
    \u0647\u0630\u0627 \u0633\u064A\u0633\u0645\u062D \u0644\u0644\u0645\u062A\u0635\u0641\u062D \u0627\u0644\u062E\u0627\u0635 \u0628\u0643 \u0628\u062A\u062D\u0645\u064A\u0644 \u0627\u0644\u0645\u0644\u0641\u0627\u062A ".wasm" \u0627\u0644\u0645\u0637\u0644\u0648\u0628\u0629.
    \u0625\u0630\u0627 \u0627\u0633\u062A\u0645\u0631\u062A \u0627\u0644\u0645\u0634\u0643\u0644\u0629\u060C \u0642\u062F \u062A\u062D\u062A\u0627\u062C \u0625\u0644\u0649 \u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0645\u062A\u0635\u0641\u062D \u0623\u062E\u0631.
error-javascript-conflict =
    \u0648\u0627\u062C\u0647\u062A \u0631\u0641\u0644 \u0645\u0634\u0643\u0644\u0629 \u0631\u0626\u064A\u0633\u064A\u0629 \u0623\u062B\u0646\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u0629 \u0627\u0644\u062A\u0647\u064A\u0626\u0629.
    \u064A\u0628\u062F\u0648 \u0623\u0646 \u0647\u0630\u0647 \u0627\u0644\u0635\u0641\u062D\u0629 \u062A\u0633\u062A\u062E\u062F\u0645 \u0643\u0648\u062F \u062C\u0627\u0641\u0627 \u0633\u0643\u0631\u064A\u0628\u062A \u0627\u0644\u0630\u064A \u064A\u062A\u0639\u0627\u0631\u0636 \u0645\u0639 \u0631\u0641\u0644.
    \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u0641\u0625\u0646\u0646\u0627 \u0646\u062F\u0639\u0648\u0643 \u0625\u0644\u0649 \u0645\u062D\u0627\u0648\u0644\u0629 \u062A\u062D\u0645\u064A\u0644 \u0627\u0644\u0645\u0644\u0641 \u0639\u0644\u0649 \u0635\u0641\u062D\u0629 \u0641\u0627\u0631\u063A\u0629.
error-javascript-conflict-outdated = \u064A\u0645\u0643\u0646\u0643 \u0623\u064A\u0636\u064B\u0627 \u0645\u062D\u0627\u0648\u0644\u0629 \u062A\u062D\u0645\u064A\u0644 \u0646\u0633\u062E\u0629 \u0623\u062D\u062F\u062B \u0645\u0646 \u0631\u0641\u0644 \u0627\u0644\u062A\u064A \u0642\u062F \u062A\u062D\u0644 \u0627\u0644\u0645\u0634\u0643\u0644\u0629 (\u0627\u0644\u0646\u0633\u062E\u0629 \u0627\u0644\u062D\u0627\u0644\u064A\u0629 \u0642\u062F\u064A\u0645\u0629: { $buildDate }).
error-csp-conflict =
    \u0648\u0627\u062C\u0647\u062A \u0631\u0641\u0644 \u0645\u0634\u0643\u0644\u0629 \u0631\u0626\u064A\u0633\u064A\u0629 \u0623\u062B\u0646\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u0629 \u0627\u0644\u062A\u0647\u064A\u0626\u0629.
    \u0644\u0627 \u062A\u0633\u0645\u062D \u0633\u064A\u0627\u0633\u0629 \u0623\u0645\u0627\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0644\u062E\u0627\u062F\u0645 \u0627\u0644\u0648\u064A\u0628 \u0647\u0630\u0627 \u0628\u062A\u0634\u063A\u064A\u0644 \u0645\u0643\u0648\u0646 ".wasm" \u0627\u0644\u0645\u0637\u0644\u0648\u0628.
    \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u064A\u0631\u062C\u0649 \u0627\u0644\u0631\u062C\u0648\u0639 \u0625\u0644\u0649 \u0631\u0641\u0644 \u0648\u064A\u0643\u064A \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u0645\u0633\u0627\u0639\u062F\u0629.
error-unknown =
    \u0648\u0627\u062C\u0647\u062A \u0631\u0648\u0644 \u0645\u0634\u0643\u0644\u0629 \u0631\u0626\u064A\u0633\u064A\u0629 \u0623\u062B\u0646\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u0629 \u0639\u0631\u0636 \u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0641\u0644\u0627\u0634 \u0647\u0630\u0627.
    { $outdated ->
        [true] \u0625\u0630\u0627 \u0643\u0646\u062A \u0645\u0633\u0624\u0648\u0644 \u0627\u0644\u062E\u0627\u062F\u0645\u060C \u0627\u0644\u0631\u062C\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u0629 \u062A\u062D\u0645\u064A\u0644 \u0625\u0635\u062F\u0627\u0631 \u0623\u062D\u062F\u062B \u0645\u0646 \u0631\u0641\u0644 (\u0627\u0644\u0646\u0633\u062E\u0629 \u0627\u0644\u062D\u0627\u0644\u064A\u0629 \u0642\u062F\u064A\u0645\u0629: { $buildDate }).
       *[false] \u0644\u064A\u0633 \u0645\u0646 \u0627\u0644\u0645\u0641\u062A\u0631\u0636 \u0623\u0646 \u064A\u062D\u062F\u062B \u0647\u0630\u0627\u060C \u0644\u0630\u0644\u0643 \u0646\u062D\u0646 \u0646\u0642\u062F\u0631 \u062D\u0642\u064B\u0627 \u0625\u0630\u0627 \u0642\u0645\u062A \u0628\u0627\u0644\u062A\u0628\u0644\u064A\u063A \u0639\u0646 \u0627\u0644\u062E\u0637\u0623!
    }
`,"save-manager.ftl":`save-delete-prompt = \u0647\u0644 \u0623\u0646\u062A \u0645\u062A\u0623\u0643\u062F \u0623\u0646\u0643 \u062A\u0631\u064A\u062F \u062D\u0630\u0641 \u0645\u0644\u0641 \u062D\u0641\u0638 \u0627\u0644\u0644\u0639\u0628\u0629 \u0647\u0630\u0627\u061F
save-reload-prompt =
    \u0627\u0644\u0637\u0631\u064A\u0642\u0629 \u0627\u0644\u0648\u062D\u064A\u062F\u0629 \u0644\u0640 { $action ->
        [delete] \u062D\u0630\u0641
       *[replace] \u0627\u0633\u062A\u0628\u062F\u0627\u0644
    } \u0647\u0630\u0627 \u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u062D\u0641\u0638 \u062F\u0648\u0646 \u062A\u0636\u0627\u0631\u0628 \u0645\u062D\u062A\u0645\u0644 \u0647\u064A \u0625\u0639\u0627\u062F\u0629 \u062A\u062D\u0645\u064A\u0644 \u0647\u0630\u0627 \u0627\u0644\u0645\u062D\u062A\u0648\u0649. \u0647\u0644 \u062A\u0631\u063A\u0628 \u0641\u064A \u0627\u0644\u0645\u062A\u0627\u0628\u0639\u0629 \u0639\u0644\u0649 \u0623\u064A \u062D\u0627\u0644\u061F
save-download = \u062A\u062D\u0645\u064A\u0644
save-replace = \u0627\u0633\u062A\u0628\u062F\u0627\u0644
save-delete = \u062D\u0630\u0641
save-backup-all = \u062A\u062D\u0645\u064A\u0644 \u062C\u0645\u064A\u0639 \u0627\u0644\u0645\u0644\u0641\u0627\u062A \u0627\u0644\u0645\u062D\u0641\u0648\u0638\u0629
`,"volume-controls.ftl":`volume-controls = \u0627\u0644\u062A\u062D\u0643\u0645 \u0628\u0627\u0644\u0635\u0648\u062A
volume-controls-mute = \u0643\u062A\u0645
volume-controls-volume = \u0645\u0633\u062A\u0648\u0649 \u0627\u0644\u0635\u0648\u062A
`},"ca-ES":{"context_menu.ftl":`context-menu-download-swf = Baixa el fitxer .swf
context-menu-copy-debug-info = Copia la informaci\xF3 de depuraci\xF3
context-menu-open-save-manager = Obre el gestor d'emmagatzematge
context-menu-about-ruffle =
    { $flavor ->
        [extension] Quant a l'extensi\xF3 de Ruffle ({ $version })
       *[other] Quant a Ruffle ({ $version })
    }
context-menu-hide = Amaga aquest men\xFA
context-menu-exit-fullscreen = Surt de la pantalla completa
context-menu-enter-fullscreen = Pantalla completa
`,"messages.ftl":`panic-title = Alguna cosa ha fallat :(
more-info = M\xE9s informaci\xF3
run-anyway = Reprodueix igualment
continue = Continua
report-bug = Informa d'un error
update-ruffle = Actualitza Ruffle
ruffle-demo = Demostraci\xF3 web
ruffle-desktop = Aplicaci\xF3 d'escriptori
ruffle-wiki = Obre la wiki de Ruffle
view-error-details = Mostra detalls de l'error
open-in-new-tab = Obre en una pestanya nova
click-to-unmute = Feu clic per activar el so
`,"save-manager.ftl":"","volume-controls.ftl":""},"cs-CZ":{"context_menu.ftl":`context-menu-download-swf = St\xE1hnout .swf
context-menu-copy-debug-info = Zkop\xEDrovat debug info
context-menu-open-save-manager = Otev\u0159\xEDt spr\xE1vce ulo\u017Een\xED
context-menu-about-ruffle =
    { $flavor ->
         [extension] O Ruffle roz\u0161\xED\u0159en\xED ({ $version })
        *[other] O Ruffle ({ $version })
    }
context-menu-hide = Skr\xFDt menu
context-menu-exit-fullscreen = Ukon\u010Dit re\u017Eim cel\xE9 obrazovky
context-menu-enter-fullscreen = P\u0159ej\xEDt do re\u017Eimu cel\xE9 obrazovky
context-menu-volume-controls = Ovl\xE1d\xE1n\xED hlasitosti
`,"messages.ftl":`message-cant-embed =
    Ruffle nemohl spustit Flash vlo\u017Een\xFD na t\xE9to str\xE1nce.
    M\u016F\u017Eete se pokusit otev\u0159\xEDt soubor na samostatn\xE9 kart\u011B, abyste se vyhnuli tomuto probl\xE9mu.
panic-title = N\u011Bco se pokazilo :(
more-info = Dal\u0161\xED informace
run-anyway = P\u0159esto spustit
continue = Pokra\u010Dovat
report-bug = Nahl\xE1sit chybu
update-ruffle = Aktualizovat Ruffle
ruffle-demo = Web Demo
ruffle-desktop = Desktopov\xE1 aplikace
ruffle-wiki = Zobrazit Ruffle Wiki
enable-hardware-acceleration = Zd\xE1 se, \u017Ee hardwarov\xE1 akcelerace nen\xED povolena. I kdy\u017E Ruffle funguje spr\xE1vn\u011B, m\u016F\u017Ee b\xFDt nep\u0159im\u011B\u0159en\u011B pomal\xFD. Jak povolit hardwarovou akceleraci zjist\xEDte na tomto odkazu.
view-error-details = Zobrazit podrobnosti o chyb\u011B
open-in-new-tab = Otev\u0159\xEDt na nov\xE9 kart\u011B
click-to-unmute = Kliknut\xEDm zru\u0161\xEDte ztlumen\xED
error-file-protocol =
    Zd\xE1 se, \u017Ee pou\u017E\xEDv\xE1te Ruffle na protokolu "file:".
    To nen\xED mo\u017En\xE9, proto\u017Ee prohl\xED\u017Ee\u010De blokuj\xED fungov\xE1n\xED mnoha funkc\xED z bezpe\u010Dnostn\xEDch d\u016Fvod\u016F.
    Nam\xEDsto toho v\xE1m doporu\u010Dujeme nastavit lok\xE1ln\xED server nebo pou\u017E\xEDt web demo \u010Di desktopovou aplikaci.
error-javascript-config =
    Ruffle narazil na probl\xE9m v d\u016Fsledku nespr\xE1vn\xE9 konfigurace JavaScriptu.
    Pokud jste spr\xE1vcem serveru, doporu\u010Dujeme v\xE1m zkontrolovat podrobnosti o chyb\u011B, abyste zjistili, kter\xFD parametr je vadn\xFD.
    Pomoc m\u016F\u017Eete z\xEDskat tak\xE9 na wiki Ruffle.
error-wasm-not-found =
    Ruffle se nepoda\u0159ilo na\u010D\xEDst po\u017Eadovanou komponentu souboru \u201E.wasm\u201C.
    Pokud jste spr\xE1vcem serveru, zkontrolujte, zda byl soubor spr\xE1vn\u011B nahr\xE1n.
    Pokud probl\xE9m p\u0159etrv\xE1v\xE1, mo\u017En\xE1 budete muset pou\u017E\xEDt nastaven\xED \u201EpublicPath\u201C: pomoc naleznete na wiki Ruffle.
error-wasm-mime-type =
    Ruffle narazil na probl\xE9m p\u0159i pokusu o inicializaci.
    Tento webov\xFD server neposkytuje soubory \u201E.wasm\u201C se spr\xE1vn\xFDm typem MIME.
    Pokud jste spr\xE1vcem serveru, n\xE1pov\u011Bdu najdete na Ruffle wiki.
error-swf-fetch =
    Ruffle se nepoda\u0159ilo na\u010D\xEDst SWF soubor Flash.
    Nejpravd\u011Bpodobn\u011Bj\u0161\xEDm d\u016Fvodem je, \u017Ee soubor ji\u017E neexistuje, tak\u017Ee Ruffle nem\xE1 co na\u010D\xEDst.
    Zkuste po\u017E\xE1dat o pomoc spr\xE1vce webu.
error-swf-cors =
    Ruffle se nepoda\u0159ilo na\u010D\xEDst SWF soubor Flash.
    P\u0159\xEDstup k na\u010D\xEDt\xE1n\xED byl pravd\u011Bpodobn\u011B zablokov\xE1n politikou CORS.
    Pokud jste spr\xE1vcem serveru, n\xE1pov\u011Bdu najdete na Ruffle wiki.
error-wasm-cors =
    Ruffle se nepoda\u0159ilo na\u010D\xEDst po\u017Eadovanou komponentu souboru \u201E.wasm\u201C.
    P\u0159\xEDstup k na\u010D\xEDt\xE1n\xED byl pravd\u011Bpodobn\u011B zablokov\xE1n politikou CORS.
    Pokud jste spr\xE1vcem serveru, n\xE1pov\u011Bdu najdete na Ruffle wiki.
error-wasm-invalid =
    Ruffle narazil na probl\xE9m p\u0159i pokusu o inicializaci.
    Zd\xE1 se, \u017Ee na t\xE9to str\xE1nce chyb\xED nebo jsou neplatn\xE9 soubory ke spu\u0161t\u011Bn\xED Ruffle.
    Pokud jste spr\xE1vcem serveru, n\xE1pov\u011Bdu najdete na Ruffle wiki.
error-wasm-download =
    Ruffle narazil na probl\xE9m p\u0159i pokusu o inicializaci.
    Probl\xE9m se m\u016F\u017Ee vy\u0159e\u0161it i s\xE1m, tak\u017Ee m\u016F\u017Eete zkusit str\xE1nku na\u010D\xEDst znovu.
    V opa\u010Dn\xE9m p\u0159\xEDpad\u011B kontaktujte administr\xE1tora str\xE1nky.
error-wasm-disabled-on-edge =
    Ruffle se nepoda\u0159ilo na\u010D\xEDst po\u017Eadovanou komponentu souboru \u201E.wasm\u201C.
    Chcete-li tento probl\xE9m vy\u0159e\u0161it, zkuste otev\u0159\xEDt nastaven\xED prohl\xED\u017Ee\u010De, klikn\u011Bte na polo\u017Eku \u201EOchrana osobn\xEDch \xFAdaj\u016F, vyhled\xE1v\xE1n\xED a slu\u017Eby\u201C, p\u0159ejd\u011Bte dol\u016F a vypn\u011Bte mo\u017Enost \u201EZvy\u0161te svou bezpe\u010Dnost na webu\u201C.
    Va\u0161emu prohl\xED\u017Ee\u010Di to umo\u017En\xED na\u010D\xEDst po\u017Eadovan\xE9 soubory \u201E.wasm\u201C.
    Pokud probl\xE9m p\u0159etrv\xE1v\xE1, budete mo\u017En\xE1 muset pou\u017E\xEDt jin\xFD prohl\xED\u017Ee\u010D.
error-javascript-conflict =
    Ruffle narazil na probl\xE9m p\u0159i pokusu o inicializaci.
    Zd\xE1 se, \u017Ee tato str\xE1nka pou\u017E\xEDv\xE1 k\xF3d JavaScript, kter\xFD je v konfliktu s Ruffle.
    Pokud jste spr\xE1vcem serveru, doporu\u010Dujeme v\xE1m zkusit na\u010D\xEDst soubor na pr\xE1zdnou str\xE1nku.
error-javascript-conflict-outdated = M\u016F\u017Eete se tak\xE9 pokusit nahr\xE1t nov\u011Bj\u0161\xED verzi Ruffle, kter\xE1 m\u016F\u017Ee dan\xFD probl\xE9m vy\u0159e\u0161it (aktu\xE1ln\xED build je zastaral\xFD: { $buildDate }).
error-csp-conflict =
    Ruffle narazil na probl\xE9m p\u0159i pokusu o inicializaci.
    Z\xE1sady zabezpe\u010Den\xED obsahu tohoto webov\xE9ho serveru nepovoluj\xED spu\u0161t\u011Bn\xED po\u017Eadovan\xE9 komponenty \u201E.wasm\u201C.
    Pokud jste spr\xE1vcem serveru, n\xE1pov\u011Bdu najdete na Ruffle wiki.
error-unknown =
    Ruffle narazil na probl\xE9m p\u0159i pokusu zobrazit tento Flash obsah.
    { $outdated ->
          [true] Pokud jste spr\xE1vcem serveru, zkuste nahr\xE1t nov\u011Bj\u0161\xED verzi Ruffle (aktu\xE1ln\xED build je zastaral\xFD: { $buildDate }).
         *[false] Toto by se nem\u011Blo st\xE1t, tak\u017Ee bychom opravdu ocenili, kdybyste mohli nahl\xE1sit chybu!
    }
`,"save-manager.ftl":`save-delete-prompt = Opravdu chcete odstranit tento soubor s ulo\u017Een\xFDmi pozicemi?
save-reload-prompt =
    Jedin\xFD zp\u016Fsob, jak { $action ->
          [delete] vymazat
         *[replace] nahradit
    } tento soubor s ulo\u017Een\xFDmi pozicemi bez potenci\xE1ln\xEDho konfliktu je op\u011Btovn\xE9 na\u010Dten\xED tohoto obsahu. Chcete p\u0159esto pokra\u010Dovat?
save-download = St\xE1hnout
save-replace = Nahradit
save-delete = Vymazat
save-backup-all = St\xE1hnout v\u0161echny soubory s ulo\u017Een\xFDmi pozicemi
`,"volume-controls.ftl":`volume-controls = Ovl\xE1d\xE1n\xED hlasitosti
volume-controls-mute = Ztlumit
volume-controls-volume = Hlasitost
`},"de-DE":{"context_menu.ftl":`context-menu-download-swf = .swf herunterladen
context-menu-copy-debug-info = Debug-Info kopieren
context-menu-open-save-manager = Dateimanager \xF6ffnen
context-menu-about-ruffle =
    { $flavor ->
        [extension] \xDCber Ruffle Erweiterung ({ $version })
       *[other] \xDCber Ruffle ({ $version })
    }
context-menu-hide = Men\xFC ausblenden
context-menu-exit-fullscreen = Vollbild verlassen
context-menu-enter-fullscreen = Vollbildmodus aktivieren
context-menu-volume-controls = Lautst\xE4rke einstellen
`,"messages.ftl":`message-cant-embed =
    Ruffle konnte den Flash in dieser Seite nicht ausf\xFChren.
    Du kannst versuchen, die Datei in einem separaten Tab zu \xF6ffnen, um dieses Problem zu umgehen.
panic-title = Etwas ist schief gelaufen
more-info = Weitere Informationen
run-anyway = Trotzdem ausf\xFChren
continue = Fortfahren
report-bug = Fehler melden
update-ruffle = Ruffle aktuallisieren
ruffle-demo = Web-Demo
ruffle-desktop = Desktop-Anwendung
ruffle-wiki = Ruffle-Wiki anzeigen
view-error-details = Fehlerdetails anzeigen
open-in-new-tab = In einem neuen Tab \xF6ffnen
click-to-unmute = Klicke zum Entmuten
error-file-protocol =
    Es scheint, dass Sie Ruffle auf dem "file:"-Protokoll ausf\xFChren.
    Dies funktioniert nicht so, als Browser viele Funktionen aus Sicherheitsgr\xFCnden blockieren.
    Stattdessen laden wir Sie ein, einen lokalen Server einzurichten oder entweder die Webdemo oder die Desktop-Anwendung zu verwenden.
error-javascript-config =
    Ruffle ist aufgrund einer falschen JavaScript-Konfiguration auf ein gro\xDFes Problem gesto\xDFen.
    Wenn du der Server-Administrator bist, laden wir dich ein, die Fehlerdetails zu \xFCberpr\xFCfen, um herauszufinden, welcher Parameter fehlerhaft ist.
    Sie k\xF6nnen auch das Ruffle-Wiki f\xFCr Hilfe konsultieren.
error-wasm-not-found =
    Ruffle konnte die erforderliche ".wasm"-Datei-Komponente nicht laden.
    Wenn Sie der Server-Administrator sind, stellen Sie bitte sicher, dass die Datei korrekt hochgeladen wurde.
    Wenn das Problem weiterhin besteht, m\xFCssen Sie unter Umst\xE4nden die "publicPath"-Einstellung verwenden: Bitte konsultieren Sie das Ruffle-Wiki f\xFCr Hilfe.
error-wasm-mime-type =
    Ruffle ist auf ein gro\xDFes Problem beim Initialisieren gesto\xDFen.
    Dieser Webserver dient nicht ". asm"-Dateien mit dem korrekten MIME-Typ.
    Wenn Sie der Server-Administrator sind, konsultieren Sie bitte das Ruffle-Wiki f\xFCr Hilfe.
error-swf-fetch =
    Ruffle konnte die Flash-SWF-Datei nicht laden.
    Der wahrscheinlichste Grund ist, dass die Datei nicht mehr existiert, so dass Ruffle nicht geladen werden kann.
    Kontaktieren Sie den Website-Administrator f\xFCr Hilfe.
error-swf-cors =
    Ruffle konnte die Flash-SWF-Datei nicht laden.
    Der Zugriff auf den Abruf wurde wahrscheinlich durch die CORS-Richtlinie blockiert.
    Wenn Sie der Server-Administrator sind, konsultieren Sie bitte das Ruffle-Wiki f\xFCr Hilfe.
error-wasm-cors =
    Ruffle konnte die Flash-SWF-Datei nicht laden.
    Der Zugriff auf den Abruf wurde wahrscheinlich durch die CORS-Richtlinie blockiert.
    Wenn Sie der Server-Administrator sind, konsultieren Sie bitte das Ruffle-Wiki f\xFCr Hilfe.
error-wasm-invalid =
    Ruffle ist auf ein gro\xDFes Problem beim Initialisieren gesto\xDFen.
    Dieser Webserver dient nicht ". asm"-Dateien mit dem korrekten MIME-Typ.
    Wenn Sie der Server-Administrator sind, konsultieren Sie bitte das Ruffle-Wiki f\xFCr Hilfe.
error-wasm-download =
    Ruffle ist auf ein gro\xDFes Problem gesto\xDFen, w\xE4hrend er versucht hat zu initialisieren.
    Dies kann sich oft selbst beheben, so dass Sie versuchen k\xF6nnen, die Seite neu zu laden.
    Andernfalls kontaktieren Sie bitte den Website-Administrator.
error-wasm-disabled-on-edge =
    Ruffle konnte die erforderliche ".wasm"-Datei-Komponente nicht laden.
    Um dies zu beheben, versuche die Einstellungen deines Browsers zu \xF6ffnen, klicke auf "Privatsph\xE4re, Suche und Dienste", scrollen nach unten und schalte "Verbessere deine Sicherheit im Web" aus.
    Dies erlaubt Ihrem Browser die erforderlichen ".wasm"-Dateien zu laden.
    Wenn das Problem weiterhin besteht, m\xFCssen Sie m\xF6glicherweise einen anderen Browser verwenden.
error-javascript-conflict =
    Ruffle ist auf ein gro\xDFes Problem beim Initialisieren gesto\xDFen.
    Es scheint, als ob diese Seite JavaScript-Code verwendet, der mit Ruffle kollidiert.
    Wenn Sie der Server-Administrator sind, laden wir Sie ein, die Datei auf einer leeren Seite zu laden.
error-javascript-conflict-outdated = Du kannst auch versuchen, eine neuere Version von Ruffle hochzuladen, die das Problem umgehen k\xF6nnte (aktuelle Version ist veraltet: { $buildDate }).
error-csp-conflict =
    Ruffle ist auf ein gro\xDFes Problem beim Initialisieren gesto\xDFen.
    Dieser Webserver dient nicht ". asm"-Dateien mit dem korrekten MIME-Typ.
    Wenn Sie der Server-Administrator sind, konsultieren Sie bitte das Ruffle-Wiki f\xFCr Hilfe.
error-unknown =
    Bei dem Versuch, diesen Flash-Inhalt anzuzeigen, ist Ruffle auf ein gro\xDFes Problem gesto\xDFen.
    { $outdated ->
        [true] Wenn Sie der Server-Administrator sind, Bitte versuchen Sie, eine neuere Version von Ruffle hochzuladen (aktuelle Version ist veraltet: { $buildDate }).
       *[false] Dies soll nicht passieren, deshalb w\xFCrden wir uns sehr dar\xFCber freuen, wenn Sie einen Fehler melden k\xF6nnten!
    }
`,"save-manager.ftl":`save-delete-prompt = Sind Sie sicher, dass Sie diese Speicherdatei l\xF6schen m\xF6chten?
save-reload-prompt =
    Der einzige Weg zu { $action ->
        [delete] l\xF6schen
       *[replace] ersetzen
    } diese Speicherdatei ohne m\xF6glichen Konflikt ist das erneute Laden dieses Inhalts. M\xF6chten Sie trotzdem fortfahren?
save-download = Herunterladen
save-replace = Ersetzen
save-delete = L\xF6schen
save-backup-all = Alle gespeicherten Dateien herunterladen
`,"volume-controls.ftl":`volume-controls = Lautst\xE4rkeeinstellungen
volume-controls-mute = Stummschalten
volume-controls-volume = Lautst\xE4rke
`},"en-US":{"context_menu.ftl":`context-menu-download-swf = Download .swf
context-menu-copy-debug-info = Copy debug info
context-menu-open-save-manager = Open Save Manager
context-menu-about-ruffle =
    { $flavor ->
        [extension] About Ruffle Extension ({$version})
        *[other] About Ruffle ({$version})
    }
context-menu-hide = Hide this menu
context-menu-exit-fullscreen = Exit fullscreen
context-menu-enter-fullscreen = Enter fullscreen
context-menu-volume-controls = Volume controls
`,"messages.ftl":`message-cant-embed =
    Ruffle wasn't able to run the Flash embedded in this page.
    You can try to open the file in a separate tab, to sidestep this issue.
panic-title = Something went wrong :(
more-info = More info
run-anyway = Run anyway
continue = Continue
report-bug = Report Bug
update-ruffle = Update Ruffle
ruffle-demo = Web Demo
ruffle-desktop = Desktop Application
ruffle-wiki = View Ruffle Wiki
enable-hardware-acceleration = It looks like hardware acceleration is not enabled. While Ruffle may work, it could be unreasonably slow. You can find out how to enable hardware acceleration by following this link.
view-error-details = View Error Details
open-in-new-tab = Open in a new tab
click-to-unmute = Click to unmute
error-file-protocol =
    It appears you are running Ruffle on the "file:" protocol.
    This doesn't work as browsers block many features from working for security reasons.
    Instead, we invite you to setup a local server or either use the web demo or the desktop application.
error-javascript-config =
    Ruffle has encountered a major issue due to an incorrect JavaScript configuration.
    If you are the server administrator, we invite you to check the error details to find out which parameter is at fault.
    You can also consult the Ruffle wiki for help.
error-wasm-not-found =
    Ruffle failed to load the required ".wasm" file component.
    If you are the server administrator, please ensure the file has correctly been uploaded.
    If the issue persists, you may need to use the "publicPath" setting: please consult the Ruffle wiki for help.
error-wasm-mime-type =
    Ruffle has encountered a major issue whilst trying to initialize.
    This web server is not serving ".wasm" files with the correct MIME type.
    If you are the server administrator, please consult the Ruffle wiki for help.
error-swf-fetch =
    Ruffle failed to load the Flash SWF file.
    The most likely reason is that the file no longer exists, so there is nothing for Ruffle to load.
    Try contacting the website administrator for help.
error-swf-cors =
    Ruffle failed to load the Flash SWF file.
    Access to fetch has likely been blocked by CORS policy.
    If you are the server administrator, please consult the Ruffle wiki for help.
error-wasm-cors =
    Ruffle failed to load the required ".wasm" file component.
    Access to fetch has likely been blocked by CORS policy.
    If you are the server administrator, please consult the Ruffle wiki for help.
error-wasm-invalid =
    Ruffle has encountered a major issue whilst trying to initialize.
    It seems like this page has missing or invalid files for running Ruffle.
    If you are the server administrator, please consult the Ruffle wiki for help.
error-wasm-download =
    Ruffle has encountered a major issue whilst trying to initialize.
    This can often resolve itself, so you can try reloading the page.
    Otherwise, please contact the website administrator.
error-wasm-disabled-on-edge =
    Ruffle failed to load the required ".wasm" file component.
    To fix this, try opening your browser's settings, clicking "Privacy, search, and services", scrolling down, and turning off "Enhance your security on the web".
    This will allow your browser to load the required ".wasm" files.
    If the issue persists, you might have to use a different browser.
error-javascript-conflict =
    Ruffle has encountered a major issue whilst trying to initialize.
    It seems like this page uses JavaScript code that conflicts with Ruffle.
    If you are the server administrator, we invite you to try loading the file on a blank page.
error-javascript-conflict-outdated = You can also try to upload a more recent version of Ruffle that may circumvent the issue (current build is outdated: {$buildDate}).
error-csp-conflict =
    Ruffle has encountered a major issue whilst trying to initialize.
    This web server's Content Security Policy does not allow the required ".wasm" component to run.
    If you are the server administrator, please consult the Ruffle wiki for help.
error-unknown =
    Ruffle has encountered a major issue whilst trying to display this Flash content.
    {$outdated ->
        [true] If you are the server administrator, please try to upload a more recent version of Ruffle (current build is outdated: {$buildDate}).
        *[false] This isn't supposed to happen, so we'd really appreciate if you could file a bug!
    }
`,"save-manager.ftl":`save-delete-prompt = Are you sure you want to delete this save file?
save-reload-prompt =
    The only way to {$action ->
    [delete] delete
    *[replace] replace
    } this save file without potential conflict is to reload this content. Do you wish to continue anyway?
save-download = Download
save-replace = Replace
save-delete = Delete
save-backup-all = Download all save files`,"volume-controls.ftl":`volume-controls = Volume controls
volume-controls-mute = Mute
volume-controls-volume = Volume
`},"es-ES":{"context_menu.ftl":`context-menu-download-swf = Descargar .swf
context-menu-copy-debug-info = Copiar Informaci\xF3n de depuraci\xF3n
context-menu-open-save-manager = Abrir gestor de guardado
context-menu-about-ruffle =
    { $flavor ->
        [extension] Sobre la extensi\xF3n de Ruffle ({ $version })
       *[other] Sobre Ruffle ({ $version })
    }
context-menu-hide = Ocultar este men\xFA
context-menu-exit-fullscreen = Salir de pantalla completa
context-menu-enter-fullscreen = Entrar a pantalla completa
context-menu-volume-controls = Controles de volumen
`,"messages.ftl":`message-cant-embed =
    Ruffle no pudo ejecutar el Flash incrustado en esta p\xE1gina.
    Puedes intentar abrir el archivo en una pesta\xF1a aparte, para evitar este problema.
panic-title = Algo sali\xF3 mal :(
more-info = M\xE1s info
run-anyway = Ejecutar de todos modos
continue = Continuar
report-bug = Reportar un Error
update-ruffle = Actualizar Ruffle
ruffle-demo = Demostraci\xF3n de web
ruffle-desktop = Aplicaci\xF3n de Desktop
ruffle-wiki = Ver la p\xE1gina wiki
enable-hardware-acceleration = Al parecer, la aceleraci\xF3n de hardware no esta habilitada. Puede que Ruffle funcione, pero ser\xE1 extremadamente lento. Puedes averiguar como habilitar la aceleraci\xF3n de hardware al entrar al enlace.
view-error-details = Ver los detalles del error
open-in-new-tab = Abrir en una pesta\xF1a nueva
click-to-unmute = Haz clic para dejar de silenciar
error-file-protocol =
    Parece que est\xE1 ejecutando Ruffle en el protocolo "archivo:".
    Esto no funciona porque los navegadores bloquean que muchas caracter\xEDsticas funcionen por razones de seguridad.
    En su lugar, le invitamos a configurar un servidor local o bien usar la demostraci\xF3n web o la aplicaci\xF3n de desktop.
error-javascript-config =
    Ruffle ha encontrado un problema cr\xEDtico debido a una configuraci\xF3n JavaScript incorrecta.
    Si usted es el administrador del servidor, le invitamos a comprobar los detalles del error para averiguar qu\xE9 par\xE1metro est\xE1 en falta.
    Tambi\xE9n puedes consultar la wiki de Ruffle para obtener ayuda.
error-wasm-not-found =
    Ruffle no pudo cargar el componente de archivo ".wasm" requerido.
    Si usted es el administrador del servidor, aseg\xFArese de que el archivo ha sido subido correctamente.
    Si el problema persiste, puede que necesite usar la configuraci\xF3n "publicPath": por favor consulte la wiki de Ruffle para obtener ayuda.
error-wasm-mime-type =
    Ruffle ha encontrado un problema cr\xEDtico al intentar inicializar.
    Este servidor web no est\xE1 sirviendo archivos wasm" con el tipo MIME correcto.
    Si usted es el administrador del servidor, consulte la wiki de Ruffle para obtener ayuda.
error-swf-fetch =
    Ruffle no pudo cargar el archivo Flash SWF.
    La raz\xF3n m\xE1s probable es que el archivo ya no existe, as\xED que no hay nada para cargar Ruffle.
    Intente ponerse en contacto con el administrador del sitio web para obtener ayuda.
error-swf-cors =
    Ruffle no pudo cargar el archivo Flash SWF.
    Es probable que el acceso a la b\xFAsqueda haya sido bloqueado por la pol\xEDtica CORS.
    Si usted es el administrador del servidor, consulte la wiki de Ruffle para obtener ayuda.
error-wasm-cors =
    Ruffle no pudo cargar el archivo ".wasm."
    Es probable que el acceso a la b\xFAsqueda o la llamada a la funci\xF3n fetch haya sido bloqueado por la pol\xEDtica CORS.
    Si usted es el administrador del servidor, consulte la wiki de Ruffle para obtener ayuda.
error-wasm-invalid =
    Ruffle ha encontrado un problema cr\xEDtico al intentar inicializar.
    Este servidor web no est\xE1 sirviendo archivos wasm" con el tipo Mime correcto.
    Si usted es el administrador del servidor, consulte la wiki de Ruffle para obtener ayuda.
error-wasm-download =
    Ruffle ha encontrado un problema cr\xEDtico mientras intentaba inicializarse.
    Esto a menudo puede resolverse por s\xED mismo, as\xED que puede intentar recargar la p\xE1gina.
    De lo contrario, p\xF3ngase en contacto con el administrador del sitio web.
error-wasm-disabled-on-edge =
    Ruffle no pudo cargar el componente de archivo ".wasm" requerido.
    Para solucionar esto, intenta abrir la configuraci\xF3n de tu navegador, haciendo clic en "Privacidad, b\xFAsqueda y servicios", desplaz\xE1ndote y apagando "Mejore su seguridad en la web".
    Esto permitir\xE1 a su navegador cargar los archivos ".wasm" necesarios.
    Si el problema persiste, puede que tenga que utilizar un navegador diferente.
error-javascript-conflict =
    Ruffle ha encontrado un problema cr\xEDtico mientras intentaba inicializarse.
    Parece que esta p\xE1gina utiliza c\xF3digo JavaScript que entra en conflicto con Ruffle.
    Si usted es el administrador del servidor, le invitamos a intentar cargar el archivo en una p\xE1gina en blanco.
error-javascript-conflict-outdated = Tambi\xE9n puedes intentar subir una versi\xF3n m\xE1s reciente de Ruffle que puede eludir el problema (la versi\xF3n actual est\xE1 desactualizada: { $buildDate }).
error-csp-conflict =
    Ruffle encontr\xF3 un problema al intentar inicializarse.
    La Pol\xEDtica de Seguridad de Contenido de este servidor web no permite el componente requerido ".wasm". 
    Si usted es el administrador del servidor, por favor consulta la wiki de Ruffle para obtener ayuda.
error-unknown =
    Ruffle ha encontrado un problema al tratar de mostrar el contenido Flash.
    { $outdated ->
        [true] Si usted es el administrador del servidor, intenta cargar una version m\xE1s reciente de Ruffle (la version actual esta desactualizada: { $buildDate }).
       *[false] Esto no deberia suceder! apreciariamos que reportes el error!
    }
`,"save-manager.ftl":`save-delete-prompt = \xBFEst\xE1 seguro de querer eliminar este archivo de guardado?
save-reload-prompt =
    La \xFAnica forma de { $action ->
        [delete] eliminar
       *[replace] sobreescribir
    } este archivo de guardado sin conflictos potenciales es reiniciando el contenido. \xBFDesea continuar de todos modos?
save-download = Descargar
save-replace = Sobreescribir
save-delete = Borrar
save-backup-all = Borrar todos los archivos de guardado
`,"volume-controls.ftl":`volume-controls = Controles de volumen
volume-controls-mute = Silenciar
volume-controls-volume = Volumen
`},"fr-FR":{"context_menu.ftl":`context-menu-download-swf = T\xE9l\xE9charger en tant que .swf
context-menu-copy-debug-info = Copier les infos de d\xE9bogage
context-menu-open-save-manager = Ouvrir le gestionnaire de stockage
context-menu-about-ruffle =
    { $flavor ->
        [extension] \xC0 propos de Ruffle Extension ({ $version })
       *[other] \xC0 propos de Ruffle ({ $version })
    }
context-menu-hide = Masquer ce menu
context-menu-exit-fullscreen = Sortir du mode plein \xE9cran
context-menu-enter-fullscreen = Afficher en plein \xE9cran
`,"messages.ftl":`message-cant-embed =
    Ruffle n'a pas \xE9t\xE9 en mesure de lire le fichier Flash int\xE9gr\xE9 dans cette page.
    Vous pouvez essayer d'ouvrir le fichier dans un onglet isol\xE9, pour contourner le probl\xE8me.
panic-title = Une erreur est survenue :(
more-info = Plus d'infos
run-anyway = Ex\xE9cuter quand m\xEAme
continue = Continuer
report-bug = Signaler le bug
update-ruffle = Mettre \xE0 jour Ruffle
ruffle-demo = D\xE9mo en ligne
ruffle-desktop = Application de bureau
ruffle-wiki = Wiki de Ruffle
view-error-details = D\xE9tails de l'erreur
open-in-new-tab = Ouvrir dans un nouvel onglet
click-to-unmute = Cliquez pour activer le son
error-file-protocol =
    Il semblerait que vous ex\xE9cutiez Ruffle sur le protocole "file:".
    Cela ne fonctionne pas car les navigateurs bloquent de nombreuses fonctionnalit\xE9s pour des raisons de s\xE9curit\xE9.
    Nous vous invitons soit \xE0 configurer un serveur local, soit \xE0 utiliser la d\xE9mo en ligne ou l'application de bureau.
error-javascript-config =
    Ruffle a rencontr\xE9 un probl\xE8me majeur en raison d'une configuration JavaScript incorrecte.
    Si vous \xEAtes l'administrateur du serveur, nous vous invitons \xE0 v\xE9rifier les d\xE9tails de l'erreur pour savoir quel est le param\xE8tre en cause.
    Vous pouvez \xE9galement consulter le wiki de Ruffle pour obtenir de l'aide.
error-wasm-not-found =
    Ruffle n'a pas r\xE9ussi \xE0 charger son fichier ".wasm".
    Si vous \xEAtes l'administrateur du serveur, veuillez vous assurer que ce fichier a bien \xE9t\xE9 mis en ligne.
    Si le probl\xE8me persiste, il vous faudra peut-\xEAtre utiliser le param\xE8tre "publicPath" : veuillez consulter le wiki de Ruffle pour obtenir de l'aide.
error-wasm-mime-type =
    Ruffle a rencontr\xE9 un probl\xE8me majeur durant sa phase d'initialisation.
    Ce serveur web ne renvoie pas le bon type MIME pour les fichiers ".wasm".
    Si vous \xEAtes l'administrateur du serveur, veuillez consulter le wiki de Ruffle pour obtenir de l'aide.
error-swf-fetch =
    Ruffle n'a pas r\xE9ussi \xE0 charger le fichier Flash.
    La raison la plus probable est que le fichier n'existe pas ou plus.
    Vous pouvez essayer de prendre contact avec l'administrateur du site pour obtenir plus d'informations.
error-swf-cors =
    Ruffle n'a pas r\xE9ussi \xE0 charger le fichier Flash.
    La requ\xEAte a probablement \xE9t\xE9 rejet\xE9e en raison de la configuration du CORS.
    Si vous \xEAtes l'administrateur du serveur, veuillez consulter le wiki de Ruffle pour obtenir de l'aide.
error-wasm-cors =
    Ruffle n'a pas r\xE9ussi \xE0 charger son fichier ".wasm".
    La requ\xEAte a probablement \xE9t\xE9 rejet\xE9e en raison de la configuration du CORS.
    Si vous \xEAtes l'administrateur du serveur, veuillez consulter le wiki de Ruffle pour obtenir de l'aide.
error-wasm-invalid =
    Ruffle a rencontr\xE9 un probl\xE8me majeur durant sa phase d'initialisation.
    Il semblerait que cette page comporte des fichiers manquants ou invalides pour ex\xE9cuter Ruffle.
    Si vous \xEAtes l'administrateur du serveur, veuillez consulter le wiki de Ruffle pour obtenir de l'aide.
error-wasm-download =
    Ruffle a rencontr\xE9 un probl\xE8me majeur durant sa phase d'initialisation.
    Le probl\xE8me d\xE9tect\xE9 peut souvent se r\xE9soudre de lui-m\xEAme, donc vous pouvez essayer de recharger la page.
    Si le probl\xE8me persiste, veuillez prendre contact avec l'administrateur du site.
error-wasm-disabled-on-edge =
    Ruffle n'a pas r\xE9ussi \xE0 charger son fichier ".wasm".
    Pour r\xE9soudre ce probl\xE8me, essayez d'ouvrir les param\xE8tres de votre navigateur et de cliquer sur "Confidentialit\xE9, recherche et services". Puis, vers le bas de la page, d\xE9sactivez l'option "Am\xE9liorez votre s\xE9curit\xE9 sur le web".
    Cela permettra \xE0 votre navigateur de charger les fichiers ".wasm".
    Si le probl\xE8me persiste, vous devrez peut-\xEAtre utiliser un autre navigateur.
error-javascript-conflict =
    Ruffle a rencontr\xE9 un probl\xE8me majeur durant sa phase d'initialisation.
    Il semblerait que cette page contienne du code JavaScript qui entre en conflit avec Ruffle.
    Si vous \xEAtes l'administrateur du serveur, nous vous invitons \xE0 essayer de charger le fichier dans une page vide.
error-javascript-conflict-outdated = Vous pouvez \xE9galement essayer de mettre en ligne une version plus r\xE9cente de Ruffle qui pourrait avoir corrig\xE9 le probl\xE8me (la version que vous utilisez est obsol\xE8te : { $buildDate }).
error-csp-conflict =
    Ruffle a rencontr\xE9 un probl\xE8me majeur durant sa phase d'initialisation.
    La strat\xE9gie de s\xE9curit\xE9 du contenu (CSP) de ce serveur web n'autorise pas l'ex\xE9cution de fichiers ".wasm".
    Si vous \xEAtes l'administrateur du serveur, veuillez consulter le wiki de Ruffle pour obtenir de l'aide.
error-unknown =
    Ruffle a rencontr\xE9 un probl\xE8me majeur durant l'ex\xE9cution de ce contenu Flash.
    { $outdated ->
        [true] Si vous \xEAtes l'administrateur du serveur, veuillez essayer de mettre en ligne une version plus r\xE9cente de Ruffle (la version que vous utilisez est obsol\xE8te : { $buildDate }).
       *[false] Cela n'est pas cens\xE9 se produire, donc nous vous serions reconnaissants si vous pouviez nous signaler ce bug !
    }
`,"save-manager.ftl":`save-delete-prompt = Voulez-vous vraiment supprimer ce fichier de sauvegarde ?
save-reload-prompt =
    La seule fa\xE7on de { $action ->
        [delete] supprimer
       *[replace] remplacer
    } ce fichier de sauvegarde sans conflit potentiel est de recharger ce contenu. Souhaitez-vous quand m\xEAme continuer ?
save-download = T\xE9l\xE9charger
save-replace = Remplacer
save-delete = Supprimer
save-backup-all = T\xE9l\xE9charger tous les fichiers de sauvegarde
`,"volume-controls.ftl":""},"he-IL":{"context_menu.ftl":`context-menu-download-swf = \u05D4\u05D5\u05E8\u05D3\u05EA \u05E7\u05D5\u05D1\u05E5 \u05D4swf.
context-menu-copy-debug-info = \u05D4\u05E2\u05EA\u05E7\u05EA \u05E0\u05EA\u05D5\u05E0\u05D9 \u05E0\u05D9\u05E4\u05D5\u05D9 \u05E9\u05D2\u05D9\u05D0\u05D5\u05EA
context-menu-open-save-manager = \u05E4\u05EA\u05D7 \u05D0\u05EA \u05DE\u05E0\u05D4\u05DC \u05D4\u05E9\u05DE\u05D9\u05E8\u05D5\u05EA
context-menu-about-ruffle =
    { $flavor ->
        [extension] \u05D0\u05D5\u05D3\u05D5\u05EA \u05D4\u05EA\u05D5\u05E1\u05E3 Ruffle ({ $version })
       *[other] \u05D0\u05D5\u05D3\u05D5\u05EA Ruffle ({ $version })
    }
context-menu-hide = \u05D4\u05E1\u05EA\u05E8 \u05EA\u05E4\u05E8\u05D9\u05D8 \u05D6\u05D4
context-menu-exit-fullscreen = \u05D9\u05E6\u05D9\u05D0\u05D4 \u05DE\u05DE\u05E1\u05DA \u05DE\u05DC\u05D0
context-menu-enter-fullscreen = \u05DE\u05E1\u05DA \u05DE\u05DC\u05D0
context-menu-volume-controls = \u05D1\u05E7\u05E8\u05EA \u05E2\u05D5\u05E6\u05DE\u05EA \u05E7\u05D5\u05DC
`,"messages.ftl":`message-cant-embed =
    Ruffle \u05DC\u05D0 \u05D4\u05E6\u05DC\u05D9\u05D7 \u05DC\u05D4\u05E8\u05D9\u05E5 \u05D0\u05EA \u05EA\u05D5\u05DB\u05DF \u05D4\u05E4\u05DC\u05D0\u05E9 \u05D4\u05DE\u05D5\u05D8\u05DE\u05E2 \u05D1\u05D3\u05E3 \u05D6\u05D4.
    \u05D0\u05EA\u05D4 \u05D9\u05DB\u05D5\u05DC \u05DC\u05E4\u05EA\u05D5\u05D7 \u05D0\u05EA \u05D4\u05E7\u05D5\u05D1\u05E5 \u05D1\u05DC\u05E9\u05D5\u05E0\u05D9\u05EA \u05E0\u05E4\u05E8\u05D3\u05EA, \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E2\u05E7\u05D5\u05E3 \u05D1\u05E2\u05D9\u05D4 \u05D6\u05D5.
panic-title = \u05DE\u05E9\u05D4\u05D5 \u05D4\u05E9\u05EA\u05D1\u05E9 :(
more-info = \u05DE\u05D9\u05D3\u05E2 \u05E0\u05D5\u05E1\u05E3
run-anyway = \u05D4\u05E4\u05E2\u05DC \u05D1\u05DB\u05DC \u05D6\u05D0\u05EA
continue = \u05D4\u05DE\u05E9\u05DA
report-bug = \u05D3\u05D5\u05D5\u05D7 \u05E2\u05DC \u05EA\u05E7\u05DC\u05D4
update-ruffle = \u05E2\u05D3\u05DB\u05DF \u05D0\u05EA Ruffle
ruffle-demo = \u05D4\u05D3\u05D2\u05DE\u05D4
ruffle-desktop = \u05D0\u05E4\u05DC\u05D9\u05E7\u05E6\u05D9\u05D9\u05EA \u05E9\u05D5\u05DC\u05D7\u05DF \u05E2\u05D1\u05D5\u05D3\u05D4
ruffle-wiki = \u05E8\u05D0\u05D4 \u05D0\u05EA Ruffle wiki
enable-hardware-acceleration = \u05E0\u05E8\u05D0\u05D4 \u05E9\u05D4\u05D0\u05E6\u05EA \u05D4\u05D7\u05D5\u05DE\u05E8\u05D4 \u05E9\u05DC\u05DA \u05DC\u05D0 \u05DE\u05D5\u05E4\u05E2\u05DC\u05EA. \u05D1\u05E2\u05D5\u05D3 \u05E9\u05E8\u05D0\u05E4\u05DC \u05E2\u05E9\u05D5\u05D9 \u05DC\u05E2\u05D1\u05D5\u05D3, \u05D4\u05D5\u05D0 \u05D9\u05DB\u05D5\u05DC \u05DC\u05D4\u05D9\u05D5\u05EA \u05D0\u05D9\u05D8\u05D9. \u05EA\u05D5\u05DB\u05DC \u05DC\u05E8\u05D0\u05D5\u05EA \u05DB\u05D9\u05E6\u05D3 \u05DC\u05D4\u05E4\u05E2\u05D9\u05DC \u05EA\u05DB\u05D5\u05E0\u05D4 \u05D6\u05D5 \u05D1\u05DC\u05D7\u05D9\u05E6\u05D4 \u05E2\u05DC \u05D4\u05DC\u05D9\u05E0\u05E7 \u05D4\u05D6\u05D4.
view-error-details = \u05E8\u05D0\u05D4 \u05E4\u05E8\u05D8\u05D9 \u05E9\u05D2\u05D9\u05D0\u05D4
open-in-new-tab = \u05E4\u05EA\u05D7 \u05D1\u05DB\u05E8\u05D8\u05D9\u05E1\u05D9\u05D9\u05D4 \u05D7\u05D3\u05E9\u05D4
click-to-unmute = \u05DC\u05D7\u05E5 \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05D1\u05D8\u05DC \u05D4\u05E9\u05EA\u05E7\u05D4
error-file-protocol =
    \u05E0\u05D3\u05DE\u05D4 \u05E9\u05D0\u05EA\u05D4 \u05DE\u05E8\u05D9\u05E5 \u05D0\u05EA Ruffle \u05EA\u05D7\u05EA \u05E4\u05E8\u05D5\u05D8\u05D5\u05E7\u05D5\u05DC "file:".
    \u05D6\u05D4 \u05DC\u05D0 \u05D9\u05E2\u05D1\u05D5\u05D3 \u05DE\u05DB\u05D9\u05D5\u05D5\u05DF \u05E9\u05D3\u05E4\u05D3\u05E4\u05E0\u05D9\u05DD \u05D7\u05D5\u05E1\u05DE\u05D9\u05DD \u05D0\u05E4\u05E9\u05E8\u05D5\u05D9\u05D5\u05EA \u05E8\u05D1\u05D5\u05EA \u05DE\u05DC\u05E2\u05D1\u05D5\u05D3 \u05E2\u05E7\u05D1 \u05E1\u05D9\u05D1\u05D5\u05EA \u05D0\u05D1\u05D8\u05D7\u05D4.
    \u05D1\u05DE\u05E7\u05D5\u05DD \u05D6\u05D4, \u05D0\u05E0\u05D5 \u05DE\u05D6\u05DE\u05D9\u05E0\u05D9\u05DD \u05D0\u05D5\u05EA\u05DA \u05DC\u05D0\u05D7\u05E1\u05DF \u05D0\u05EA\u05E8 \u05D6\u05D4 \u05EA\u05D7\u05EA \u05E9\u05E8\u05EA \u05DE\u05E7\u05D5\u05DE\u05D9 \u05D0\u05D5 \u05D4\u05D3\u05D2\u05DE\u05D4 \u05D1\u05E8\u05E9\u05EA \u05D0\u05D5 \u05D3\u05E8\u05DA \u05D0\u05E4\u05DC\u05D9\u05E7\u05E6\u05D9\u05D9\u05EA \u05E9\u05D5\u05DC\u05D7\u05DF \u05D4\u05E2\u05D1\u05D5\u05D3\u05D4.
error-javascript-config =
    Ruffle \u05E0\u05EA\u05E7\u05DC \u05D1\u05EA\u05E7\u05DC\u05D4 \u05D7\u05DE\u05D5\u05E8\u05D4 \u05E2\u05E7\u05D1 \u05D4\u05D2\u05D3\u05E8\u05EA JavaScript \u05E9\u05D2\u05D5\u05D9\u05D4.
    \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D5 \u05DE\u05D6\u05DE\u05D9\u05E0\u05D9\u05DD \u05D0\u05D5\u05EA\u05DA \u05DC\u05D1\u05D3\u05D5\u05E7 \u05D0\u05EA \u05E4\u05E8\u05D8\u05D9 \u05D4\u05E9\u05D2\u05D9\u05D0\u05D4 \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05DE\u05E6\u05D5\u05D0 \u05D0\u05D9\u05D6\u05D4 \u05E4\u05E8\u05DE\u05D8\u05E8 \u05D4\u05D5\u05D0 \u05E9\u05D2\u05D5\u05D9.
    \u05D0\u05EA\u05D4 \u05D9\u05DB\u05D5\u05DC \u05DC\u05E2\u05D9\u05D9\u05DF \u05D5\u05DC\u05D4\u05D5\u05E2\u05E5 \u05D1wiki \u05E9\u05DC Ruffle \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E7\u05D1\u05DC \u05E2\u05D6\u05E8\u05D4.
error-wasm-not-found =
    Ruffle \u05E0\u05DB\u05E9\u05DC \u05DC\u05D8\u05E2\u05D5\u05DF \u05D0\u05EA \u05E7\u05D5\u05D1\u05E5 \u05D4"wasm." \u05D4\u05D3\u05E8\u05D5\u05E9.
    \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D0 \u05D5\u05D5\u05D3\u05D0 \u05DB\u05D9 \u05D4\u05E7\u05D5\u05D1\u05E5 \u05D4\u05D5\u05E2\u05DC\u05D4 \u05DB\u05E9\u05D5\u05E8\u05D4.
    \u05D0\u05DD \u05D4\u05D1\u05E2\u05D9\u05D4 \u05DE\u05DE\u05E9\u05D9\u05DB\u05D4, \u05D9\u05D9\u05EA\u05DB\u05DF \u05D5\u05EA\u05E6\u05D8\u05E8\u05DA \u05DC\u05D4\u05E9\u05EA\u05DE\u05E9 \u05D1\u05D4\u05D2\u05D3\u05E8\u05EA "publicPath": \u05D0\u05E0\u05D0 \u05E2\u05D9\u05D9\u05DF \u05D5\u05D4\u05D5\u05E2\u05E5 \u05D1wiki \u05E9\u05DC Ruffle \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E7\u05D1\u05DC \u05E2\u05D6\u05E8\u05D4.
error-wasm-mime-type =
    Ruffle \u05E0\u05EA\u05E7\u05DC \u05D1\u05D1\u05E2\u05D9\u05D4 \u05D7\u05DE\u05D5\u05E8\u05D4 \u05EA\u05D5\u05DA \u05DB\u05D3\u05D9 \u05E0\u05D9\u05E1\u05D9\u05D5\u05DF \u05DC\u05D0\u05EA\u05D7\u05DC.
    \u05E9\u05E8\u05EA\u05D5 \u05E9\u05DC \u05D0\u05EA\u05E8 \u05D6\u05D4 \u05DC\u05D0 \u05DE\u05E9\u05D9\u05D9\u05DA \u05E7\u05D1\u05E6\u05D9 ".wasm" \u05E2\u05DD \u05E1\u05D5\u05D2 \u05D4MIME \u05D4\u05E0\u05DB\u05D5\u05DF.
    \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D0 \u05E2\u05D9\u05D9\u05DF \u05D5\u05D4\u05D5\u05E2\u05E5 \u05D1wiki \u05E9\u05DC Ruffle \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E7\u05D1\u05DC \u05E2\u05D6\u05E8\u05D4.
error-swf-fetch =
    Ruffle \u05E0\u05DB\u05E9\u05DC \u05DC\u05D8\u05E2\u05D5\u05DF \u05D0\u05EA \u05E7\u05D5\u05D1\u05E5 \u05D4\u05E4\u05DC\u05D0\u05E9/swf. .
    \u05D6\u05D4 \u05E0\u05D5\u05D1\u05E2 \u05DB\u05DB\u05DC \u05D4\u05E0\u05E8\u05D0\u05D4 \u05DE\u05DB\u05D9\u05D5\u05D5\u05DF \u05D5\u05D4\u05E7\u05D5\u05D1\u05E5 \u05DC\u05D0 \u05E7\u05D9\u05D9\u05DD \u05D9\u05D5\u05EA\u05E8, \u05D0\u05D6 \u05D0\u05D9\u05DF \u05DCRuffle \u05DE\u05D4 \u05DC\u05D8\u05E2\u05D5\u05DF.
    \u05E0\u05E1\u05D4 \u05DC\u05D9\u05E6\u05D5\u05E8 \u05E7\u05E9\u05E8 \u05E2\u05DD \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8 \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E7\u05D1\u05DC \u05E2\u05D6\u05E8\u05D4.
error-swf-cors =
    Ruffle \u05E0\u05DB\u05E9\u05DC \u05DC\u05D8\u05E2\u05D5\u05DF \u05D0\u05EA \u05E7\u05D5\u05D1\u05E5 \u05D4\u05E4\u05DC\u05D0\u05E9/swf. .
    \u05D2\u05D9\u05E9\u05D4 \u05DCfetch \u05DB\u05DB\u05DC \u05D4\u05E0\u05E8\u05D0\u05D4 \u05E0\u05D7\u05E1\u05DE\u05D4 \u05E2\u05DC \u05D9\u05D3\u05D9 \u05DE\u05D3\u05D9\u05E0\u05D9\u05D5\u05EA CORS.
    \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D0 \u05E2\u05D9\u05D9\u05DF \u05D5\u05D4\u05D5\u05E2\u05E5 \u05D1wiki \u05E9\u05DC Ruffle \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E7\u05D1\u05DC \u05E2\u05D6\u05E8\u05D4.
error-wasm-cors =
    Ruffle \u05E0\u05DB\u05E9\u05DC \u05DC\u05D8\u05E2\u05D5\u05DF \u05D0\u05EA \u05E7\u05D5\u05D1\u05E5 \u05D4".wasm" \u05D4\u05D3\u05E8\u05D5\u05E9.
    \u05D2\u05D9\u05E9\u05D4 \u05DCfetch \u05DB\u05DB\u05DC \u05D4\u05E0\u05E8\u05D0\u05D4 \u05E0\u05D7\u05E1\u05DE\u05D4 \u05E2\u05DC \u05D9\u05D3\u05D9 \u05DE\u05D3\u05D9\u05E0\u05D9\u05D5\u05EA CORS.
    \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D0 \u05E2\u05D9\u05D9\u05DF \u05D5\u05D4\u05D5\u05E2\u05E5 \u05D1wiki \u05E9\u05DC Ruffle \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E7\u05D1\u05DC \u05E2\u05D6\u05E8\u05D4.
error-wasm-invalid =
    Ruffle \u05E0\u05EA\u05E7\u05DC \u05D1\u05D1\u05E2\u05D9\u05D4 \u05D7\u05DE\u05D5\u05E8\u05D4 \u05EA\u05D5\u05DA \u05DB\u05D3\u05D9 \u05E0\u05D9\u05E1\u05D9\u05D5\u05DF \u05DC\u05D0\u05EA\u05D7\u05DC.
    \u05E0\u05D3\u05DE\u05D4 \u05DB\u05D9 \u05D1\u05D3\u05E3 \u05D6\u05D4 \u05D7\u05E1\u05E8\u05D9\u05DD \u05D0\u05D5 \u05DC\u05D0 \u05E2\u05D5\u05D1\u05D3\u05D9\u05DD \u05DB\u05E8\u05D0\u05D5\u05D9 \u05E7\u05D1\u05E6\u05D9\u05DD \u05D0\u05E9\u05E8 \u05DE\u05E9\u05DE\u05E9\u05D9\u05DD \u05D0\u05EA Ruffle \u05DB\u05D3\u05D9 \u05DC\u05E4\u05E2\u05D5\u05DC
    \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D0 \u05E2\u05D9\u05D9\u05DF \u05D5\u05D4\u05D5\u05E2\u05E5 \u05D1wiki \u05E9\u05DC Ruffle \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E7\u05D1\u05DC \u05E2\u05D6\u05E8\u05D4.
error-wasm-download =
    Ruffle \u05E0\u05EA\u05E7\u05DC \u05D1\u05D1\u05E2\u05D9\u05D4 \u05D7\u05DE\u05D5\u05E8\u05D4 \u05EA\u05D5\u05DA \u05DB\u05D3\u05D9 \u05E0\u05D9\u05E1\u05D9\u05D5\u05DF \u05DC\u05D0\u05EA\u05D7\u05DC.
    \u05DC\u05E2\u05D9\u05EA\u05D9\u05DD \u05D1\u05E2\u05D9\u05D4 \u05D6\u05D5 \u05D9\u05DB\u05D5\u05DC\u05D4 \u05DC\u05E4\u05EA\u05D5\u05E8 \u05D0\u05EA \u05E2\u05E6\u05DE\u05D4, \u05D0\u05D6 \u05D0\u05EA\u05D4 \u05D9\u05DB\u05D5\u05DC \u05DC\u05E0\u05E1\u05D5\u05EA \u05DC\u05D8\u05E2\u05D5\u05DF \u05DE\u05D7\u05D3\u05E9 \u05D0\u05EA \u05D4\u05D3\u05E3 \u05D6\u05D4.
    \u05D0\u05DD \u05DC\u05D0, \u05D0\u05E0\u05D0 \u05E4\u05E0\u05D4 \u05DC\u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8.
error-wasm-disabled-on-edge =
    Ruffle \u05E0\u05DB\u05E9\u05DC \u05DC\u05D8\u05E2\u05D5\u05DF \u05D0\u05EA \u05E7\u05D5\u05D1\u05E5 \u05D4".wasm" \u05D4\u05D3\u05E8\u05D5\u05E9.
    \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05EA\u05E7\u05DF \u05D1\u05E2\u05D9\u05D4 \u05D6\u05D5, \u05E0\u05E1\u05D4 \u05DC\u05E4\u05EA\u05D5\u05D7 \u05D0\u05EA \u05D4\u05D2\u05D3\u05E8\u05D5\u05EA \u05D4\u05D3\u05E4\u05D3\u05E4\u05DF \u05E9\u05DC\u05DA, \u05DC\u05D7\u05E5 \u05E2\u05DC "\u05D0\u05D1\u05D8\u05D7\u05D4, \u05D7\u05D9\u05E4\u05D5\u05E9 \u05D5\u05E9\u05D9\u05E8\u05D5\u05EA",
    \u05D2\u05DC\u05D5\u05DC \u05DE\u05D8\u05D4, \u05D5\u05DB\u05D1\u05D4 \u05D0\u05EA "\u05D4\u05D2\u05D1\u05E8 \u05D0\u05EA \u05D4\u05D0\u05D1\u05D8\u05D7\u05D4 \u05E9\u05DC\u05DA \u05D1\u05E8\u05E9\u05EA".
    \u05D6\u05D4 \u05D9\u05D0\u05E4\u05E9\u05E8 \u05DC\u05D3\u05E4\u05D3\u05E4\u05DF \u05E9\u05DC\u05DA \u05DC\u05D8\u05E2\u05D5\u05DF \u05D0\u05EA \u05E7\u05D5\u05D1\u05E5 \u05D4".wasm" \u05D4\u05D3\u05E8\u05D5\u05E9.
    \u05D0\u05DD \u05D4\u05D1\u05E2\u05D9\u05D4 \u05DE\u05DE\u05E9\u05D9\u05DB\u05D4, \u05D9\u05D9\u05EA\u05DB\u05DF \u05D5\u05E2\u05DC\u05D9\u05DA \u05DC\u05D4\u05E9\u05EA\u05DE\u05E9 \u05D1\u05D3\u05E4\u05D3\u05E4\u05DF \u05D0\u05D7\u05E8.
error-javascript-conflict =
    Ruffle \u05E0\u05EA\u05E7\u05DC \u05D1\u05D1\u05E2\u05D9\u05D4 \u05D7\u05DE\u05D5\u05E8\u05D4 \u05EA\u05D5\u05DA \u05DB\u05D3\u05D9 \u05E0\u05D9\u05E1\u05D9\u05D5\u05DF \u05DC\u05D0\u05EA\u05D7\u05DC.
    \u05E0\u05D3\u05DE\u05D4 \u05DB\u05D9 \u05D3\u05E3 \u05D6\u05D4 \u05DE\u05E9\u05EA\u05DE\u05E9 \u05D1\u05E7\u05D5\u05D3 JavaScript \u05D0\u05E9\u05E8 \u05DE\u05EA\u05E0\u05D2\u05E9 \u05E2\u05DD Ruffle.
    \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D5 \u05DE\u05D6\u05DE\u05D9\u05E0\u05D9\u05DD \u05D0\u05D5\u05EA\u05DA \u05DC\u05E0\u05E1\u05D5\u05EA \u05DC\u05D8\u05E2\u05D5\u05DF \u05D0\u05EA \u05D4\u05D3\u05E3 \u05EA\u05D7\u05EA \u05E2\u05DE\u05D5\u05D3 \u05E8\u05D9\u05E7.
error-javascript-conflict-outdated = \u05D1\u05E0\u05D5\u05E1\u05E3, \u05D0\u05EA\u05D4 \u05D9\u05DB\u05D5\u05DC \u05DC\u05E0\u05E1\u05D5\u05EA \u05D5\u05DC\u05D4\u05E2\u05DC\u05D5\u05EA \u05D2\u05E8\u05E1\u05D0\u05D5\u05EA \u05E2\u05D3\u05DB\u05E0\u05D9\u05D5\u05EA \u05E9\u05DC Ruffle \u05D0\u05E9\u05E8 \u05E2\u05DC\u05D5\u05DC\u05D9\u05DD \u05DC\u05E2\u05E7\u05D5\u05E3 \u05D1\u05E2\u05D9\u05D4 \u05D6\u05D5 (\u05D2\u05E8\u05E1\u05D4 \u05D6\u05D5 \u05D4\u05D9\u05E0\u05D4 \u05DE\u05D9\u05D5\u05E9\u05E0\u05EA : { $buildDate }).
error-csp-conflict =
    Ruffle \u05E0\u05EA\u05E7\u05DC \u05D1\u05D1\u05E2\u05D9\u05D4 \u05D7\u05DE\u05D5\u05E8\u05D4 \u05EA\u05D5\u05DA \u05DB\u05D3\u05D9 \u05E0\u05D9\u05E1\u05D9\u05D5\u05DF \u05DC\u05D0\u05EA\u05D7\u05DC.
    \u05DE\u05D3\u05D9\u05E0\u05D9\u05D5\u05EA \u05D0\u05D1\u05D8\u05D7\u05EA \u05D4\u05EA\u05D5\u05DB\u05DF \u05E9\u05DC \u05E9\u05E8\u05EA\u05D5 \u05E9\u05DC \u05D0\u05EA\u05E8 \u05D6\u05D4 \u05D0\u05D9\u05E0\u05D4 \u05DE\u05D0\u05E4\u05E9\u05E8\u05EA \u05DC\u05E7\u05D5\u05D1\u05E5 \u05D4"wasm." \u05D4\u05D3\u05E8\u05D5\u05E9 \u05DC\u05E4\u05E2\u05D5\u05DC.
    \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D0 \u05E2\u05D9\u05D9\u05DF \u05D5\u05D4\u05D5\u05E2\u05E5 \u05D1wiki \u05E9\u05DC Ruffle \u05E2\u05DC \u05DE\u05E0\u05EA \u05DC\u05E7\u05D1\u05DC \u05E2\u05D6\u05E8\u05D4.
error-unknown =
    Ruffle \u05E0\u05EA\u05E7\u05DC \u05D1\u05D1\u05E2\u05D9\u05D4 \u05D7\u05DE\u05D5\u05E8\u05D4 \u05D1\u05E0\u05D9\u05E1\u05D9\u05D5\u05DF \u05DC\u05D4\u05E6\u05D9\u05D2 \u05D0\u05EA \u05EA\u05D5\u05DB\u05DF \u05E4\u05DC\u05D0\u05E9 \u05D6\u05D4.
    { $outdated ->
        [true] \u05D0\u05DD \u05D0\u05EA\u05D4 \u05DE\u05E0\u05D4\u05DC \u05D4\u05D0\u05EA\u05E8, \u05D0\u05E0\u05D0 \u05E0\u05E1\u05D4 \u05DC\u05D4\u05E2\u05DC\u05D5\u05EA \u05D2\u05E8\u05E1\u05D4 \u05E2\u05D3\u05DB\u05E0\u05D9\u05EA \u05D9\u05D5\u05EA\u05E8 \u05E9\u05DC Ruffle (\u05D2\u05E8\u05E1\u05D4 \u05D6\u05D5 \u05D4\u05D9\u05E0\u05D4 \u05DE\u05D9\u05D5\u05E9\u05E0\u05EA:  { $buildDate }).
       *[false] \u05D6\u05D4 \u05DC\u05D0 \u05D0\u05DE\u05D5\u05E8 \u05DC\u05E7\u05E8\u05D5\u05EA, \u05E0\u05E9\u05DE\u05D7 \u05D0\u05DD \u05EA\u05D5\u05DB\u05DC \u05DC\u05E9\u05EA\u05E3 \u05EA\u05E7\u05DC\u05D4 \u05D6\u05D5!
    }
`,"save-manager.ftl":`save-delete-prompt = \u05D4\u05D0\u05DD \u05D0\u05EA\u05D4 \u05D1\u05D8\u05D5\u05D7 \u05E9\u05D1\u05E8\u05E6\u05D5\u05E0\u05DA \u05DC\u05DE\u05D7\u05D5\u05E7 \u05D0\u05EA \u05E7\u05D5\u05D1\u05E5 \u05E9\u05DE\u05D9\u05E8\u05D4 \u05D6\u05D4?
save-reload-prompt =
    \u05D4\u05D3\u05E8\u05DA \u05D4\u05D9\u05D7\u05D9\u05D3\u05D4 { $action ->
        [delete] \u05DC\u05DE\u05D7\u05D5\u05E7
       *[replace] \u05DC\u05D4\u05D7\u05DC\u05D9\u05E3
    } \u05D0\u05EA \u05E7\u05D5\u05D1\u05E5 \u05D4\u05E9\u05DE\u05D9\u05E8\u05D4 \u05D4\u05D6\u05D4 \u05DE\u05D1\u05DC\u05D9 \u05DC\u05D2\u05E8\u05D5\u05DD \u05DC\u05D5 \u05DC\u05D4\u05EA\u05E0\u05D2\u05E9 \u05D4\u05D9\u05D0 \u05DC\u05D8\u05E2\u05D5\u05DF \u05DE\u05D7\u05D3\u05E9 \u05D0\u05EA \u05EA\u05D5\u05DB\u05DF \u05D6\u05D4. \u05D4\u05D0\u05DD \u05D0\u05EA\u05D4 \u05E8\u05D5\u05E6\u05D4 \u05DC\u05D4\u05DE\u05E9\u05D9\u05DA \u05D1\u05DB\u05DC \u05D6\u05D0\u05EA?
save-download = \u05D4\u05D5\u05E8\u05D3\u05D4
save-replace = \u05D4\u05D7\u05DC\u05E4\u05D4
save-delete = \u05DE\u05D7\u05D9\u05E7\u05D4
save-backup-all = \u05D4\u05D5\u05E8\u05D3\u05EA \u05DB\u05DC \u05E7\u05D1\u05E6\u05D9 \u05D4\u05E9\u05DE\u05D9\u05E8\u05D4
`,"volume-controls.ftl":`volume-controls = \u05D1\u05E7\u05E8\u05EA \u05E2\u05D5\u05E6\u05DE\u05EA \u05E7\u05D5\u05DC
volume-controls-mute = \u05D4\u05E9\u05EA\u05E7
volume-controls-volume = \u05E2\u05D5\u05E6\u05DE\u05EA \u05E7\u05D5\u05DC
`},"hu-HU":{"context_menu.ftl":`context-menu-download-swf = .swf f\xE1jl let\xF6lt\xE9se
context-menu-copy-debug-info = Hibakeres\xE9si inform\xE1ci\xF3k m\xE1sol\xE1sa
context-menu-open-save-manager = Ment\xE9skezel\u0151 megnyit\xE1sa
context-menu-about-ruffle =
    { $flavor ->
        [extension] A Ruffle kieg\xE9sz\xEDt\u0151 ({ $version }) n\xE9vjegye
       *[other] A Ruffle ({ $version }) n\xE9vjegye
    }
context-menu-hide = Ezen men\xFC elrejt\xE9se
context-menu-exit-fullscreen = Kil\xE9p\xE9s a teljes k\xE9perny\u0151b\u0151l
context-menu-enter-fullscreen = V\xE1lt\xE1s teljes k\xE9perny\u0151re
context-menu-volume-controls = Hanger\u0151szab\xE1lyz\xF3
`,"messages.ftl":`message-cant-embed =
    A Ruffle nem tudta futtatni az oldalba \xE1gyazott Flash tartalmat.
    A probl\xE9ma kiker\xFCl\xE9s\xE9hez megpr\xF3b\xE1lhatod megnyitni a f\xE1jlt egy k\xFCl\xF6n lapon.
panic-title = Valami baj t\xF6rt\xE9nt :(
more-info = Tov\xE1bbi inform\xE1ci\xF3
run-anyway = Futtat\xE1s m\xE9gis
continue = Folytat\xE1s
report-bug = Hiba jelent\xE9se
update-ruffle = Ruffle friss\xEDt\xE9se
ruffle-demo = Webes dem\xF3
ruffle-desktop = Asztali alkalmaz\xE1s
ruffle-wiki = Ruffle Wiki megnyit\xE1sa
enable-hardware-acceleration = \xDAgy t\u0171nik, a hardveres gyors\xEDt\xE1s nincs enged\xE9lyezve. B\xE1r a Ruffle m\u0171k\xF6dhet, nagyon lass\xFA lehet. Ezt a hivatkoz\xE1st k\xF6vetve megtudhatod, hogyan enged\xE9lyezd a hardveres gyors\xEDt\xE1st.
view-error-details = Hiba r\xE9szletei
open-in-new-tab = Megnyit\xE1s \xFAj lapon
click-to-unmute = Kattints a n\xE9m\xEDt\xE1s felold\xE1s\xE1hoz
error-file-protocol =
    \xDAgy t\u0171nik, a Ruffle-t a "file:" protokollon futtatod.
    Ez nem m\u0171k\xF6dik, mivel \xEDgy a b\xF6ng\xE9sz\u0151k biztons\xE1gi okokb\xF3l sz\xE1mos funkci\xF3 m\u0171k\xF6d\xE9s\xE9t letiltj\xE1k.
    Ehelyett azt aj\xE1nljuk hogy ind\xEDts egy helyi kiszolg\xE1l\xF3t, vagy haszn\xE1ld a webes dem\xF3t vagy az asztali alkalmaz\xE1st.
error-javascript-config =
    A Ruffle komoly probl\xE9m\xE1ba \xFCtk\xF6z\xF6tt egy helytelen JavaScript-konfigur\xE1ci\xF3 miatt.
    Ha a szerver rendszergazd\xE1ja vagy, k\xE9rj\xFCk, ellen\u0151rizd a hiba r\xE9szleteit, hogy megtudd, melyik param\xE9ter a hib\xE1s.
    A Ruffle wikiben is tal\xE1lhatsz ehhez seg\xEDts\xE9get.
error-wasm-not-found =
    A Ruffle nem tudta bet\xF6lteni a sz\xFCks\xE9ges ".wasm" \xF6sszetev\u0151t.
    Ha a szerver rendszergazd\xE1ja vagy, k\xE9rj\xFCk ellen\u0151rizd, hogy a f\xE1jl megfelel\u0151en lett-e felt\xF6ltve.
    Ha a probl\xE9ma tov\xE1bbra is fenn\xE1ll, el\u0151fordulhat, hogy a "publicPath" be\xE1ll\xEDt\xE1st kell haszn\xE1lnod: seg\xEDts\xE9g\xE9rt keresd fel a Ruffle wikit.
error-wasm-mime-type =
    A Ruffle komoly probl\xE9m\xE1ba \xFCtk\xF6z\xF6tt az inicializ\xE1l\xE1s sor\xE1n.
    Ez a webszerver a ".wasm" f\xE1jlokat nem a megfelel\u0151 MIME-t\xEDpussal szolg\xE1lja ki.
    Ha a szerver rendszergazd\xE1ja vagy, k\xE9rj\xFCk, keresd fel a Ruffle wikit seg\xEDts\xE9g\xE9rt.
error-swf-fetch =
    A Ruffle nem tudta bet\xF6lteni a Flash SWF f\xE1jlt.
    A legval\xF3sz\xEDn\u0171bb ok az, hogy a f\xE1jl m\xE1r nem l\xE9tezik, \xEDgy a Ruffle sz\xE1m\xE1ra nincs mit bet\xF6lteni.
    Pr\xF3b\xE1ld meg felvenni a kapcsolatot a webhely rendszergazd\xE1j\xE1val seg\xEDts\xE9g\xE9rt.
error-swf-cors =
    A Ruffle nem tudta bet\xF6lteni a Flash SWF f\xE1jlt.
    A lek\xE9r\xE9shez val\xF3 hozz\xE1f\xE9r\xE9st val\xF3sz\xEDn\u0171leg letiltotta a CORS-h\xE1zirend.
    Ha a szerver rendszergazd\xE1ja vagy, k\xE9rj\xFCk, keresd fel a Ruffle wikit seg\xEDts\xE9g\xE9rt.
error-wasm-cors =
    A Ruffle nem tudta bet\xF6lteni a sz\xFCks\xE9ges ".wasm" \xF6sszetev\u0151t.
    A lek\xE9r\xE9shez val\xF3 hozz\xE1f\xE9r\xE9st val\xF3sz\xEDn\u0171leg letiltotta a CORS-h\xE1zirend.
    Ha a szerver rendszergazd\xE1ja vagy, k\xE9rj\xFCk keresd fel a Ruffle wikit seg\xEDts\xE9g\xE9rt.
error-wasm-invalid =
    A Ruffle komoly probl\xE9m\xE1ba \xFCtk\xF6z\xF6tt az inicializ\xE1l\xE1s sor\xE1n.
    \xDAgy t\u0171nik, hogy ezen az oldalon hi\xE1nyoznak vagy hib\xE1sak a Ruffle futtat\xE1s\xE1hoz sz\xFCks\xE9ges f\xE1jlok.
    Ha a szerver rendszergazd\xE1ja vagy, k\xE9rj\xFCk keresd fel a Ruffle wikit seg\xEDts\xE9g\xE9rt.
error-wasm-download =
    A Ruffle komoly probl\xE9m\xE1ba \xFCtk\xF6z\xF6tt az inicializ\xE1l\xE1s sor\xE1n.
    Ez gyakran mag\xE1t\xF3l megold\xF3dik, ez\xE9rt megpr\xF3b\xE1lhatod \xFAjrat\xF6lteni az oldalt.
    Ellenkez\u0151 esetben fordulj a webhely rendszergazd\xE1j\xE1hoz.
error-wasm-disabled-on-edge =
    A Ruffle nem tudta bet\xF6lteni a sz\xFCks\xE9ges ".wasm" \xF6sszetev\u0151t.
    A probl\xE9ma megold\xE1s\xE1hoz nyisd meg a b\xF6ng\xE9sz\u0151 be\xE1ll\xEDt\xE1sait, kattints az \u201EAdatv\xE9delem, keres\xE9s \xE9s szolg\xE1ltat\xE1sok\u201D elemre, g\xF6rgess le, \xE9s kapcsold ki a \u201EFokozott biztons\xE1g a weben\u201D opci\xF3t.
    Ez lehet\u0151v\xE9 teszi a b\xF6ng\xE9sz\u0151 sz\xE1m\xE1ra, hogy bet\xF6ltse a sz\xFCks\xE9ges ".wasm" f\xE1jlokat.
    Ha a probl\xE9ma tov\xE1bbra is fenn\xE1ll, lehet, hogy m\xE1sik b\xF6ng\xE9sz\u0151t kell haszn\xE1lnod.
error-javascript-conflict =
    A Ruffle komoly probl\xE9m\xE1ba \xFCtk\xF6z\xF6tt az inicializ\xE1l\xE1s sor\xE1n.
    \xDAgy t\u0171nik, ez az oldal olyan JavaScript-k\xF3dot haszn\xE1l, amely \xFCtk\xF6zik a Ruffle-lel.
    Ha a kiszolg\xE1l\xF3 rendszergazd\xE1ja vagy, k\xE9rj\xFCk, pr\xF3b\xE1ld meg a f\xE1jlt egy \xFCres oldalon bet\xF6lteni.
error-javascript-conflict-outdated = Megpr\xF3b\xE1lhatod tov\xE1bb\xE1 felt\xF6lteni a Ruffle egy \xFAjabb verzi\xF3j\xE1t is, amely megker\xFClheti a probl\xE9m\xE1t (a jelenlegi elavult: { $buildDate }).
error-csp-conflict =
    A Ruffle komoly probl\xE9m\xE1ba \xFCtk\xF6z\xF6tt az inicializ\xE1l\xE1s sor\xE1n.
    A kiszolg\xE1l\xF3 tartalombiztons\xE1gi h\xE1zirendje nem teszi lehet\u0151v\xE9 a sz\xFCks\xE9ges \u201E.wasm\u201D \xF6sszetev\u0151k futtat\xE1s\xE1t.
    Ha a szerver rendszergazd\xE1ja vagy, k\xE9rj\xFCk, keresd fel a Ruffle wikit seg\xEDts\xE9g\xE9rt.
error-unknown =
    A Ruffle komoly probl\xE9m\xE1ba \xFCtk\xF6z\xF6tt, mik\xF6zben megpr\xF3b\xE1lta megjelen\xEDteni ezt a Flash-tartalmat.
    { $outdated ->
        [true] Ha a szerver rendszergazd\xE1ja vagy, k\xE9rj\xFCk, pr\xF3b\xE1ld meg felt\xF6lteni a Ruffle egy \xFAjabb verzi\xF3j\xE1t (a jelenlegi elavult: { $buildDate }).
       *[false] Ennek nem lett volna szabad megt\xF6rt\xE9nnie, ez\xE9rt nagyon h\xE1l\xE1sak lenn\xE9nk, ha jelezn\xE9d a hib\xE1t!
    }
`,"save-manager.ftl":`save-delete-prompt = Biztosan t\xF6r\xF6lni akarod ezt a ment\xE9st?
save-reload-prompt =
    Ennek a ment\xE9snek az esetleges konfliktus n\xE9lk\xFCli { $action ->
        [delete] t\xF6rl\xE9s\xE9hez
       *[replace] cser\xE9j\xE9hez
    } \xFAjra kell t\xF6lteni a tartalmat. M\xE9gis szeretn\xE9d folytatni?
save-download = Let\xF6lt\xE9s
save-replace = Csere
save-delete = T\xF6rl\xE9s
save-backup-all = Az \xF6sszes f\xE1jl let\xF6lt\xE9se
`,"volume-controls.ftl":`volume-controls = Hanger\u0151szab\xE1lyz\xF3
volume-controls-mute = N\xE9m\xEDt\xE1s
volume-controls-volume = Hanger\u0151
`},"id-ID":{"context_menu.ftl":`context-menu-download-swf = Unduh .swf
context-menu-copy-debug-info = Salin info debug
context-menu-open-save-manager = Buka Manager Save
context-menu-about-ruffle =
    { $flavor ->
        [extension] Tentang Ekstensi Ruffle ({ $version })
       *[other] Tentang Ruffle ({ $version })
    }
context-menu-hide = Sembunyikan Menu ini
context-menu-exit-fullscreen = Keluar dari layar penuh
context-menu-enter-fullscreen = Masuk mode layar penuh
`,"messages.ftl":`message-cant-embed =
    Ruffle tidak dapat menjalankan Flash yang disematkan di halaman ini.
    Anda dapat mencoba membuka file di tab terpisah, untuk menghindari masalah ini.
panic-title = Terjadi kesalahan :(
more-info = Info lebih lanjut
run-anyway = Jalankan
continue = Lanjutkan
report-bug = Laporkan Bug
update-ruffle = Perbarui Ruffle
ruffle-demo = Demo Web
ruffle-desktop = Aplikasi Desktop
ruffle-wiki = Kunjungi Wiki Ruffle
view-error-details = Tunjukan Detail Error
open-in-new-tab = Buka di Tab Baru
click-to-unmute = Tekan untuk menyalakan suara
error-file-protocol =
    Sepertinya anda menjalankan Ruffle di protokol "file:". 
    Ini tidak berfungsi karena browser memblokir fitur ini dengan alasan keamanan.
    Sebagai gantinya, kami mengajak anda untuk membuat server lokal, menggunakan demo web atau aplikasi desktop.
error-javascript-config =
    Ruffle mengalami masalah besar karena konfigurasi JavaScript yang salah.
    Jika Anda adalah administrator server ini, kami mengajak Anda untuk memeriksa detail kesalahan untuk mengetahui parameter mana yang salah.
    Anda juga dapat membaca wiki Ruffle untuk mendapatkan bantuan.
error-wasm-not-found =
    Ruffle gagal memuat komponen file ".wasm" yang diperlukan.
    Jika Anda adalah administrator server ini, pastikan file telah diunggah dengan benar.
    Jika masalah terus berlanjut, Anda mungkin perlu menggunakan pengaturan "publicPath": silakan baca wiki Ruffle untuk mendapatkan bantuan.
error-wasm-mime-type =
    Ruffle mengalami masalah ketika mencoba melakukan inisialisasi.
    Server web ini tidak melayani file ".wasm" dengan tipe MIME yang benar.
    Jika Anda adalah administrator server ini, silakan baca wiki Ruffle untuk mendapatkan bantuan.
error-swf-fetch =
    Ruffle gagal memuat file SWF Flash.
    Kemungkinan file tersebut sudah tidak ada, sehingga tidak dapat dimuat oleh Ruffle.
    Coba hubungi administrator situs web ini untuk mendapatkan bantuan.
error-swf-cors =
    Ruffle gagal memuat file SWF Flash.
    Akses untuk memuat kemungkinan telah diblokir oleh kebijakan CORS.
    Jika Anda adalah administrator server ini, silakan baca wiki Ruffle untuk mendapatkan bantuan.
error-wasm-cors =
    Ruffle gagal memuat komponen file ".wasm" yang diperlukan.
    Akses untuk mengambil kemungkinan telah diblokir oleh kebijakan CORS.
    Jika Anda adalah administrator server ini, silakan baca wiki Ruffle untuk mendapatkan bantuan.
error-wasm-invalid =
    Ruffle mengalami masalah besar ketika mencoba melakukan inisialisasi.
    Sepertinya halaman ini memiliki file yang hilang atau tidak valid untuk menjalankan Ruffle.
    Jika Anda adalah administrator server ini, silakan baca wiki Ruffle untuk mendapatkan bantuan.
error-wasm-download =
    Ruffle mengalami masalah besar ketika mencoba melakukan inisialisasi.
    Hal ini sering kali dapat teratasi dengan sendirinya, sehingga Anda dapat mencoba memuat ulang halaman.
    Jika tidak, silakan hubungi administrator situs web ini.
error-wasm-disabled-on-edge =
    Ruffle gagal memuat komponen file ".wasm" yang diperlukan.
    Untuk mengatasinya, coba buka pengaturan peramban Anda, klik "Privasi, pencarian, dan layanan", turun ke bawah, dan matikan "Tingkatkan keamanan Anda di web".
    Ini akan memungkinkan browser Anda memuat file ".wasm" yang diperlukan.
    Jika masalah berlanjut, Anda mungkin harus menggunakan browser yang berbeda.
error-javascript-conflict =
    Ruffle mengalami masalah besar ketika mencoba melakukan inisialisasi.
    Sepertinya situs web ini menggunakan kode JavaScript yang bertentangan dengan Ruffle.
    Jika Anda adalah administrator server ini, kami mengajak Anda untuk mencoba memuat file pada halaman kosong.
error-javascript-conflict-outdated = Anda juga dapat mencoba mengunggah versi Ruffle yang lebih baru yang mungkin dapat mengatasi masalah ini (versi saat ini sudah kedaluwarsa: { $buildDate }).
error-csp-conflict =
    Ruffle mengalami masalah besar ketika mencoba melakukan inisialisasi.
    Kebijakan Keamanan Konten server web ini tidak mengizinkan komponen ".wasm" yang diperlukan untuk dijalankan.
    Jika Anda adalah administrator server ini, silakan baca wiki Ruffle untuk mendapatkan bantuan.
error-unknown =
    Ruffle telah mengalami masalah besar saat menampilkan konten Flash ini.
    { $outdated ->
        [true] Jika Anda administrator server ini, cobalah untuk mengganti versi Ruffle yang lebih baru (versi saat ini sudah kedaluwarsa: { $buildDate }).
       *[false] Hal ini seharusnya tidak terjadi, jadi kami sangat menghargai jika Anda dapat melaporkan bug ini!
    }
`,"save-manager.ftl":`save-delete-prompt = Anda yakin ingin menghapus berkas ini?
save-reload-prompt =
    Satu-satunya cara untuk { $action ->
        [delete] menghapus
       *[replace] mengganti
    } berkas penyimpanan ini tanpa potensi konflik adalah dengan memuat ulang konten ini. Apakah Anda ingin melanjutkannya?
save-download = Unduh
save-replace = Ganti
save-delete = Hapus
save-backup-all = Unduh semua berkas penyimpanan
`,"volume-controls.ftl":""},"it-IT":{"context_menu.ftl":`context-menu-download-swf = Scarica .swf
context-menu-copy-debug-info = Copia informazioni di debug
context-menu-open-save-manager = Apri Gestione salvataggi
context-menu-about-ruffle =
    { $flavor ->
        [extension] Informazioni su Ruffle Extension ({ $version })
       *[other] Informazioni su Ruffle ({ $version })
    }
context-menu-hide = Nascondi questo menu
context-menu-exit-fullscreen = Esci dallo schermo intero
context-menu-enter-fullscreen = Entra a schermo intero
`,"messages.ftl":`message-cant-embed =
    Ruffle non \xE8 stato in grado di eseguire il Flash incorporato in questa pagina.
    Puoi provare ad aprire il file in una scheda separata, per evitare questo problema.
panic-title = Qualcosa \xE8 andato storto :(
more-info = Maggiori informazioni
run-anyway = Esegui comunque
continue = Continua
report-bug = Segnala Un Bug
update-ruffle = Aggiorna Ruffle
ruffle-demo = Demo Web
ruffle-desktop = Applicazione Desktop
ruffle-wiki = Visualizza Ruffle Wiki
view-error-details = Visualizza Dettagli Errore
open-in-new-tab = Apri in una nuova scheda
click-to-unmute = Clicca per riattivare l'audio
error-file-protocol =
    Sembra che tu stia eseguendo Ruffle sul protocollo "file:".
    Questo non funziona come browser blocca molte funzionalit\xE0 di lavoro per motivi di sicurezza.
    Invece, ti invitiamo a configurare un server locale o a utilizzare la demo web o l'applicazione desktop.
error-javascript-config =
    Ruffle ha incontrato un problema importante a causa di una configurazione JavaScript non corretta.
    Se sei l'amministratore del server, ti invitiamo a controllare i dettagli dell'errore per scoprire quale parametro \xE8 in errore.
    Puoi anche consultare il wiki Ruffle per aiuto.
error-wasm-not-found =
    Ruffle non \xE8 riuscito a caricare il componente di file ".wasm".
    Se sei l'amministratore del server, assicurati che il file sia stato caricato correttamente.
    Se il problema persiste, potrebbe essere necessario utilizzare l'impostazione "publicPath": si prega di consultare il wiki Ruffle per aiuto.
error-wasm-mime-type =
    Ruffle ha incontrato un problema importante durante il tentativo di inizializzazione.
    Questo server web non serve ". asm" file con il tipo MIME corretto.
    Se sei l'amministratore del server, consulta la wiki Ruffle per aiuto.
error-swf-fetch =
    Ruffle non \xE8 riuscito a caricare il file Flash SWF.
    La ragione pi\xF9 probabile \xE8 che il file non esiste pi\xF9, quindi non c'\xE8 nulla che Ruffle possa caricare.
    Prova a contattare l'amministratore del sito web per aiuto.
error-swf-cors =
    Ruffle non \xE8 riuscito a caricare il file SWF Flash.
    L'accesso al recupero probabilmente \xE8 stato bloccato dalla politica CORS.
    Se sei l'amministratore del server, consulta la wiki Ruffle per ricevere aiuto.
error-wasm-cors =
    Ruffle non \xE8 riuscito a caricare il componente di file ".wasm".
    L'accesso al recupero probabilmente \xE8 stato bloccato dalla politica CORS.
    Se sei l'amministratore del server, consulta la wiki Ruffle per ricevere aiuto.
error-wasm-invalid =
    Ruffle ha incontrato un problema importante durante il tentativo di inizializzazione.
    Sembra che questa pagina abbia file mancanti o non validi per l'esecuzione di Ruffle.
    Se sei l'amministratore del server, consulta la wiki Ruffle per ricevere aiuto.
error-wasm-download =
    Ruffle ha incontrato un problema importante durante il tentativo di inizializzazione.
    Questo pu\xF2 spesso risolversi da solo, quindi puoi provare a ricaricare la pagina.
    Altrimenti, contatta l'amministratore del sito.
error-wasm-disabled-on-edge =
    Ruffle non ha caricato il componente di file ".wasm" richiesto.
    Per risolvere il problema, prova ad aprire le impostazioni del tuo browser, facendo clic su "Privacy, search, and services", scorrendo verso il basso e disattivando "Migliora la tua sicurezza sul web".
    Questo permetter\xE0 al tuo browser di caricare i file ".wasm" richiesti.
    Se il problema persiste, potresti dover usare un browser diverso.
error-javascript-conflict =
    Ruffle ha riscontrato un problema importante durante il tentativo di inizializzazione.
    Sembra che questa pagina utilizzi il codice JavaScript che \xE8 in conflitto con Ruffle.
    Se sei l'amministratore del server, ti invitiamo a provare a caricare il file su una pagina vuota.
error-javascript-conflict-outdated = Puoi anche provare a caricare una versione pi\xF9 recente di Ruffle che potrebbe aggirare il problema (l'attuale build \xE8 obsoleta: { $buildDate }).
error-csp-conflict =
    Ruffle ha incontrato un problema importante durante il tentativo di inizializzare.
    La Politica di Sicurezza dei Contenuti di questo server web non consente l'impostazione richiesta". asm" componente da eseguire.
    Se sei l'amministratore del server, consulta la Ruffle wiki per aiuto.
error-unknown =
    Ruffle ha incontrato un problema importante durante il tentativo di visualizzare questo contenuto Flash.
    { $outdated ->
        [true] Se sei l'amministratore del server, prova a caricare una versione pi\xF9 recente di Ruffle (la versione attuale \xE8 obsoleta: { $buildDate }).
       *[false] Questo non dovrebbe accadere, quindi ci piacerebbe molto se si potesse inviare un bug!
    }
`,"save-manager.ftl":`save-delete-prompt = Sei sicuro di voler eliminare questo file di salvataggio?
save-reload-prompt =
    L'unico modo per { $action ->
        [delete] delete
       *[replace] replace
    } questo salvataggio file senza potenziali conflitti \xE8 quello di ricaricare questo contenuto. Volete continuare comunque?
save-download = Scarica
save-replace = Sostituisci
save-delete = Elimina
save-backup-all = Scarica tutti i file di salvataggio
`,"volume-controls.ftl":""},"ja-JP":{"context_menu.ftl":`context-menu-download-swf = .swf\u3092\u30C0\u30A6\u30F3\u30ED\u30FC\u30C9
context-menu-copy-debug-info = \u30C7\u30D0\u30C3\u30B0\u60C5\u5831\u3092\u30B3\u30D4\u30FC
context-menu-open-save-manager = \u30BB\u30FC\u30D6\u30DE\u30CD\u30FC\u30B8\u30E3\u30FC\u3092\u958B\u304F
context-menu-about-ruffle =
    { $flavor ->
        [extension] Ruffle\u62E1\u5F35\u6A5F\u80FD\u306B\u3064\u3044\u3066 ({ $version })
       *[other] Ruffle\u306B\u3064\u3044\u3066 ({ $version })
    }
context-menu-hide = \u30E1\u30CB\u30E5\u30FC\u3092\u96A0\u3059
context-menu-exit-fullscreen = \u30D5\u30EB\u30B9\u30AF\u30EA\u30FC\u30F3\u3092\u7D42\u4E86
context-menu-enter-fullscreen = \u30D5\u30EB\u30B9\u30AF\u30EA\u30FC\u30F3\u306B\u3059\u308B
context-menu-volume-controls = \u97F3\u91CF\u8ABF\u7BC0
`,"messages.ftl":`message-cant-embed =
    Ruffle\u306F\u3053\u306E\u30DA\u30FC\u30B8\u306B\u57CB\u3081\u8FBC\u307E\u308C\u305F Flash \u3092\u5B9F\u884C\u3067\u304D\u307E\u305B\u3093\u3067\u3057\u305F\u3002
    \u5225\u306E\u30BF\u30D6\u3067\u30D5\u30A1\u30A4\u30EB\u3092\u958B\u304F\u3053\u3068\u3067\u3001\u3053\u306E\u554F\u984C\u3092\u89E3\u6C7A\u3067\u304D\u308B\u304B\u3082\u3057\u308C\u307E\u305B\u3093\u3002
panic-title = \u30A8\u30E9\u30FC\u304C\u767A\u751F\u3057\u307E\u3057\u305F :(
more-info = \u8A73\u7D30\u60C5\u5831
run-anyway = \u3068\u306B\u304B\u304F\u5B9F\u884C\u3059\u308B
continue = \u7D9A\u884C
report-bug = \u30D0\u30B0\u3092\u5831\u544A
update-ruffle = Ruffle\u3092\u66F4\u65B0
ruffle-demo = Web\u30C7\u30E2
ruffle-desktop = \u30C7\u30B9\u30AF\u30C8\u30C3\u30D7\u30A2\u30D7\u30EA\u30B1\u30FC\u30B7\u30E7\u30F3
ruffle-wiki = Ruffle Wiki\u3092\u8868\u793A
enable-hardware-acceleration = \u30CF\u30FC\u30C9\u30A6\u30A7\u30A2\u30A2\u30AF\u30BB\u30E9\u30EC\u30FC\u30B7\u30E7\u30F3\u304C\u6709\u52B9\u306B\u306A\u3063\u3066\u3044\u306A\u3044\u3088\u3046\u3067\u3059\u3002Ruffle\u304C\u52D5\u4F5C\u3057\u306A\u3044\u304B\u3001\u52D5\u4F5C\u304C\u9045\u304F\u306A\u308B\u53EF\u80FD\u6027\u304C\u3042\u308A\u307E\u3059\u3002 \u30CF\u30FC\u30C9\u30A6\u30A7\u30A2\u30A2\u30AF\u30BB\u30E9\u30EC\u30FC\u30B7\u30E7\u30F3\u3092\u6709\u52B9\u306B\u3059\u308B\u65B9\u6CD5\u306B\u3064\u3044\u3066\u306F\u3001\u3053\u3061\u3089\u306E\u30EA\u30F3\u30AF\u3092\u53C2\u7167\u3057\u3066\u304F\u3060\u3055\u3044\u3002
view-error-details = \u30A8\u30E9\u30FC\u306E\u8A73\u7D30\u3092\u8868\u793A
open-in-new-tab = \u65B0\u3057\u3044\u30BF\u30D6\u3067\u958B\u304F
click-to-unmute = \u30AF\u30EA\u30C3\u30AF\u3067\u30DF\u30E5\u30FC\u30C8\u3092\u89E3\u9664
error-file-protocol =
    Ruffle\u3092"file:"\u30D7\u30ED\u30C8\u30B3\u30EB\u3067\u4F7F\u7528\u3057\u3066\u3044\u308B\u3088\u3046\u3067\u3059\u3002
    \u30D6\u30E9\u30A6\u30B6\u306F\u30BB\u30AD\u30E5\u30EA\u30C6\u30A3\u4E0A\u306E\u7406\u7531\u304B\u3089\u6B86\u3069\u306E\u6A5F\u80FD\u3092\u5236\u9650\u3057\u3066\u3044\u308B\u305F\u3081\u3001\u6B63\u3057\u304F\u52D5\u4F5C\u3057\u307E\u305B\u3093\u3002
    \u30ED\u30FC\u30AB\u30EB\u30B5\u30FC\u30D0\u30FC\u3092\u30BB\u30C3\u30C8\u30A2\u30C3\u30D7\u3059\u308B\u304B\u3001\u30A6\u30A7\u30D6\u30C7\u30E2\u307E\u305F\u306F\u30C7\u30B9\u30AF\u30C8\u30C3\u30D7\u30A2\u30D7\u30EA\u3092\u3054\u5229\u7528\u304F\u3060\u3055\u3044\u3002
error-javascript-config =
    JavaScript\u306E\u8A2D\u5B9A\u304C\u6B63\u3057\u304F\u306A\u3044\u305F\u3081\u3001Ruffle\u3067\u554F\u984C\u304C\u767A\u751F\u3057\u307E\u3057\u305F\u3002
    \u30B5\u30FC\u30D0\u30FC\u7BA1\u7406\u8005\u306E\u65B9\u306F\u3001\u30A8\u30E9\u30FC\u306E\u8A73\u7D30\u304B\u3089\u3001\u3069\u306E\u30D1\u30E9\u30E1\u30FC\u30BF\u30FC\u306B\u554F\u984C\u304C\u3042\u308B\u306E\u304B\u3092\u78BA\u8A8D\u3057\u3066\u304F\u3060\u3055\u3044\u3002
    Ruffle\u306Ewiki\u3092\u53C2\u7167\u3059\u308B\u3053\u3068\u3067\u3001\u89E3\u6C7A\u65B9\u6CD5\u304C\u898B\u3064\u304B\u308B\u304B\u3082\u3057\u308C\u307E\u305B\u3093\u3002
error-wasm-not-found =
    Ruffle\u306E\u521D\u671F\u5316\u6642\u306B\u91CD\u5927\u306A\u554F\u984C\u304C\u767A\u751F\u3057\u307E\u3057\u305F\u3002
    \u3053\u306EWeb\u30B5\u30FC\u30D0\u30FC\u306E\u30B3\u30F3\u30C6\u30F3\u30C4\u30BB\u30AD\u30E5\u30EA\u30C6\u30A3\u30DD\u30EA\u30B7\u30FC\u304C\u3001\u5B9F\u884C\u306B\u5FC5\u8981\u3068\u306A\u308B\u300C.wasm\u300D\u30B3\u30F3\u30DD\u30FC\u30CD\u30F3\u30C8\u306E\u5B9F\u884C\u3092\u8A31\u53EF\u3057\u3066\u3044\u307E\u305B\u3093\u3002\u30B5\u30FC\u30D0\u30FC\u306E\u7BA1\u7406\u8005\u306E\u5834\u5408\u306F\u3001\u30D5\u30A1\u30A4\u30EB\u304C\u6B63\u3057\u304F\u30A2\u30C3\u30D7\u30ED\u30FC\u30C9\u3055\u308C\u3066\u3044\u308B\u304B\u78BA\u8A8D\u3092\u3057\u3066\u304F\u3060\u3055\u3044\u3002\u3053\u306E\u554F\u984C\u304C\u89E3\u6C7A\u3057\u306A\u3044\u5834\u5408\u306F\u3001\u300CpublicPath\u300D\u306E\u8A2D\u5B9A\u3092\u4F7F\u7528\u3059\u308B\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059\u3002
    \u30B5\u30FC\u30D0\u30FC\u306E\u7BA1\u7406\u8005\u306F\u3001Ruffle\u306Ewiki\u3092\u53C2\u7167\u3057\u3066\u304F\u3060\u3055\u3044\u3002
error-wasm-mime-type =
    Ruffle\u306E\u521D\u671F\u5316\u306B\u5931\u6557\u3059\u308B\u5927\u304D\u306A\u554F\u984C\u304C\u767A\u751F\u3057\u307E\u3057\u305F\u3002
    \u3053\u306EWeb\u30B5\u30FC\u30D0\u30FC\u306F\u6B63\u3057\u3044MIME\u30BF\u30A4\u30D7\u306E\u300C.wasm\u300D\u30D5\u30A1\u30A4\u30EB\u3092\u63D0\u4F9B\u3057\u3066\u3044\u307E\u305B\u3093\u3002
    \u30B5\u30FC\u30D0\u30FC\u306E\u7BA1\u7406\u8005\u306F\u3001Ruffle\u306Ewiki\u3092\u53C2\u7167\u3057\u3066\u304F\u3060\u3055\u3044\u3002
error-swf-fetch =
    Ruffle\u304CFlash SWF\u30D5\u30A1\u30A4\u30EB\u306E\u8AAD\u307F\u8FBC\u307F\u306B\u5931\u6557\u3057\u307E\u3057\u305F\u3002
    \u6700\u3082\u8003\u3048\u3089\u308C\u308B\u539F\u56E0\u306F\u3001SWF\u30D5\u30A1\u30A4\u30EB\u304C\u65E2\u306B\u5B58\u5728\u3057\u306A\u3044\u4E8B\u3067Ruffle\u304C\u8AAD\u307F\u8FBC\u307F\u306B\u5931\u6557\u3059\u308B\u3068\u3044\u3046\u554F\u984C\u3067\u3059\u3002
    Web\u30B5\u30A4\u30C8\u306E\u7BA1\u7406\u8005\u306B\u304A\u554F\u3044\u5408\u308F\u305B\u304F\u3060\u3055\u3044\u3002
error-swf-cors =
    Ruffle\u306FSWF\u30D5\u30A1\u30A4\u30EB\u306E\u8AAD\u307F\u8FBC\u307F\u306B\u5931\u6557\u3057\u307E\u3057\u305F\u3002
    CORS\u30DD\u30EA\u30B7\u30FC\u306E\u8A2D\u5B9A\u306B\u3088\u308A\u3001fetch\u3078\u306E\u30A2\u30AF\u30BB\u30B9\u304C\u30D6\u30ED\u30C3\u30AF\u3055\u308C\u3066\u3044\u308B\u53EF\u80FD\u6027\u304C\u3042\u308A\u307E\u3059\u3002
    \u30B5\u30FC\u30D0\u30FC\u7BA1\u7406\u8005\u306E\u65B9\u306F\u3001Ruffle\u306Ewiki\u3092\u53C2\u7167\u3057\u3066\u304F\u3060\u3055\u3044\u3002
error-wasm-cors =
    Ruffle\u306B\u5FC5\u8981\u3068\u306A\u308B\u300C.wasm\u300D\u30D5\u30A1\u30A4\u30EB\u30B3\u30F3\u30DD\u30FC\u30CD\u30F3\u30C8\u306E\u8AAD\u307F\u8FBC\u307F\u306B\u5931\u6557\u3057\u307E\u3057\u305F\u3002
    CORS\u30DD\u30EA\u30B7\u30FC\u306B\u3088\u3063\u3066fetch\u3078\u306E\u30A2\u30AF\u30BB\u30B9\u304C\u30D6\u30ED\u30C3\u30AF\u3055\u308C\u3066\u3044\u308B\u53EF\u80FD\u6027\u304C\u3042\u308A\u307E\u3059\u3002
    \u30B5\u30FC\u30D0\u30FC\u306E\u7BA1\u7406\u8005\u306F\u3001Ruffle wiki\u3092\u53C2\u7167\u3057\u3066\u304F\u3060\u3055\u3044\u3002
error-wasm-invalid =
    Ruffle\u306E\u521D\u671F\u5316\u6642\u306B\u91CD\u5927\u306A\u554F\u984C\u304C\u767A\u751F\u3057\u307E\u3057\u305F\u3002
    \u3053\u306E\u30DA\u30FC\u30B8\u306B\u306FRuffle\u3092\u5B9F\u884C\u3059\u308B\u305F\u3081\u306E\u30D5\u30A1\u30A4\u30EB\u304C\u5B58\u5728\u3057\u306A\u3044\u304B\u3001\u7121\u52B9\u306A\u30D5\u30A1\u30A4\u30EB\u304C\u3042\u308B\u304B\u3082\u3057\u308C\u307E\u305B\u3093\u3002
    \u30B5\u30FC\u30D0\u30FC\u306E\u7BA1\u7406\u8005\u306F\u3001Ruffle\u306Ewiki\u3092\u53C2\u7167\u3057\u3066\u304F\u3060\u3055\u3044\u3002
error-wasm-download =
    Ruffle\u306E\u521D\u671F\u5316\u6642\u306B\u91CD\u5927\u306A\u554F\u984C\u304C\u767A\u751F\u3057\u307E\u3057\u305F\u3002
    \u3053\u306E\u554F\u984C\u306F\u30DA\u30FC\u30B8\u3092\u518D\u8AAD\u307F\u8FBC\u307F\u3059\u308B\u4E8B\u3067\u5927\u62B5\u306F\u89E3\u6C7A\u3059\u308B\u306F\u305A\u306A\u306E\u3067\u884C\u306A\u3063\u3066\u307F\u3066\u304F\u3060\u3055\u3044\u3002
    \u3082\u3057\u3082\u89E3\u6C7A\u3057\u306A\u3044\u5834\u5408\u306F\u3001Web\u30B5\u30A4\u30C8\u306E\u7BA1\u7406\u8005\u306B\u304A\u554F\u3044\u5408\u308F\u305B\u304F\u3060\u3055\u3044\u3002
error-wasm-disabled-on-edge =
    Ruffle\u306B\u5FC5\u8981\u3068\u306A\u308B\u300C.wasm\u300D\u30D5\u30A1\u30A4\u30EB\u30B3\u30F3\u30DD\u30FC\u30CD\u30F3\u30C8\u306E\u8AAD\u307F\u8FBC\u307F\u306B\u5931\u6557\u3057\u307E\u3057\u305F\u3002
    \u3053\u306E\u554F\u984C\u3092\u89E3\u6C7A\u3059\u308B\u306B\u306F\u30D6\u30E9\u30A6\u30B6\u30FC\u306E\u8A2D\u5B9A\u3092\u958B\u304D\u3001\u300C\u30D7\u30E9\u30A4\u30D0\u30B7\u30FC\u3001\u691C\u7D22\u3001\u30B5\u30FC\u30D3\u30B9\u300D\u3092\u30AF\u30EA\u30C3\u30AF\u3057\u3001\u4E0B\u306B\u30B9\u30AF\u30ED\u30FC\u30EB\u3067\u300CWeb\u4E0A\u306E\u30BB\u30AD\u30E5\u30EA\u30C6\u30A3\u3092\u5F37\u5316\u3059\u308B\u300D\u3092\u30AA\u30D5\u306B\u3057\u3066\u307F\u3066\u304F\u3060\u3055\u3044\u3002
    \u3053\u308C\u3067\u5FC5\u8981\u3068\u306A\u308B\u300C.wasm\u300D\u30D5\u30A1\u30A4\u30EB\u304C\u8AAD\u307F\u8FBC\u307E\u308C\u308B\u3088\u3046\u306B\u306A\u308A\u307E\u3059\u3002
    \u305D\u308C\u3067\u3082\u554F\u984C\u304C\u89E3\u6C7A\u3057\u306A\u3044\u5834\u5408\u3001\u5225\u306E\u30D6\u30E9\u30A6\u30B6\u30FC\u3092\u4F7F\u7528\u3059\u308B\u5FC5\u8981\u304C\u3042\u308B\u304B\u3082\u3057\u308C\u307E\u305B\u3093\u3002
error-javascript-conflict =
    Ruffle\u306E\u521D\u671F\u5316\u6642\u306B\u91CD\u5927\u306A\u554F\u984C\u304C\u767A\u751F\u3057\u307E\u3057\u305F\u3002
    \u3053\u306E\u30DA\u30FC\u30B8\u3067\u306FRuffle\u3068\u7AF6\u5408\u3059\u308BJavaScript\u30B3\u30FC\u30C9\u304C\u4F7F\u7528\u3055\u308C\u3066\u3044\u308B\u304B\u3082\u3057\u308C\u307E\u305B\u3093\u3002
    \u30B5\u30FC\u30D0\u30FC\u306E\u7BA1\u7406\u8005\u306F\u3001\u7A7A\u767D\u306E\u30DA\u30FC\u30B8\u3067\u30D5\u30A1\u30A4\u30EB\u3092\u8AAD\u307F\u8FBC\u307F\u3057\u76F4\u3057\u3066\u307F\u3066\u304F\u3060\u3055\u3044\u3002
error-javascript-conflict-outdated = \u65B0\u3057\u3044\u30D0\u30FC\u30B8\u30E7\u30F3\u306ERuffle\u3092\u30A2\u30C3\u30D7\u30ED\u30FC\u30C9\u3059\u308B\u3053\u3068\u3067\u3001\u3053\u306E\u554F\u984C\u3092\u56DE\u907F\u3067\u304D\u308B\u53EF\u80FD\u6027\u304C\u3042\u308A\u307E\u3059\u3002(\u73FE\u5728\u306E\u30D3\u30EB\u30C9\u306F\u53E4\u3044\u7269\u3067\u3059:{ $buildDate })
error-csp-conflict =
    Ruffle\u306E\u521D\u671F\u5316\u6642\u306B\u91CD\u5927\u306A\u554F\u984C\u304C\u767A\u751F\u3057\u307E\u3057\u305F\u3002
    \u3053\u306EWeb\u30B5\u30FC\u30D0\u30FC\u306E\u30B3\u30F3\u30C6\u30F3\u30C4\u30BB\u30AD\u30E5\u30EA\u30C6\u30A3\u30DD\u30EA\u30B7\u30FC\u304C\u5B9F\u884C\u306B\u5FC5\u8981\u3068\u306A\u308B\u300C.wasm\u300D\u30B3\u30F3\u30DD\u30FC\u30CD\u30F3\u30C8\u306E\u5B9F\u884C\u3092\u8A31\u53EF\u3057\u3066\u3044\u307E\u305B\u3093\u3002
    \u30B5\u30FC\u30D0\u30FC\u306E\u7BA1\u7406\u8005\u306F\u3001Ruffle\u306Ewiki\u3092\u53C2\u7167\u3057\u3066\u304F\u3060\u3055\u3044\u3002
error-unknown =
    Flash\u30B3\u30F3\u30C6\u30F3\u30C4\u3092\u8868\u793A\u3059\u308B\u969B\u306BRuffle\u3067\u554F\u984C\u304C\u767A\u751F\u3057\u307E\u3057\u305F\u3002
    { $outdated ->
        [true] \u73FE\u5728\u4F7F\u7528\u3057\u3066\u3044\u308B\u30D3\u30EB\u30C9\u306F\u6700\u65B0\u3067\u306F\u306A\u3044\u305F\u3081\u3001\u30B5\u30FC\u30D0\u30FC\u7BA1\u7406\u8005\u306E\u65B9\u306F\u3001\u6700\u65B0\u7248\u306ERuffle\u306B\u66F4\u65B0\u3057\u3066\u307F\u3066\u304F\u3060\u3055\u3044(\u73FE\u5728\u5229\u7528\u4E2D\u306E\u30D3\u30EB\u30C9: { $buildDate })\u3002
       *[false] \u60F3\u5B9A\u5916\u306E\u554F\u984C\u306A\u306E\u3067\u3001\u30D0\u30B0\u3068\u3057\u3066\u5831\u544A\u3057\u3066\u3044\u305F\u3060\u3051\u308B\u3068\u5B09\u3057\u3044\u3067\u3059!
    }
`,"save-manager.ftl":`save-delete-prompt = \u3053\u306E\u30BB\u30FC\u30D6\u30D5\u30A1\u30A4\u30EB\u3092\u524A\u9664\u3057\u3066\u3082\u3088\u308D\u3057\u3044\u3067\u3059\u304B?
save-reload-prompt =
    \u30BB\u30FC\u30D6\u30D5\u30A1\u30A4\u30EB\u3092\u7AF6\u5408\u306E\u53EF\u80FD\u6027\u306A\u304F { $action ->
        [delete] \u524A\u9664\u3059\u308B
       *[replace] \u7F6E\u304D\u63DB\u3048\u308B
    } \u305F\u3081\u306B\u3001\u3053\u306E\u30B3\u30F3\u30C6\u30F3\u30C4\u3092\u518D\u8AAD\u307F\u8FBC\u307F\u3059\u308B\u3053\u3068\u3092\u63A8\u5968\u3057\u307E\u3059\u3002\u7D9A\u884C\u3057\u307E\u3059\u304B\uFF1F
save-download = \u30C0\u30A6\u30F3\u30ED\u30FC\u30C9
save-replace = \u7F6E\u304D\u63DB\u3048
save-delete = \u524A\u9664
save-backup-all = \u3059\u3079\u3066\u306E\u30BB\u30FC\u30D6\u30D5\u30A1\u30A4\u30EB\u3092\u30C0\u30A6\u30F3\u30ED\u30FC\u30C9
`,"volume-controls.ftl":`volume-controls = \u97F3\u91CF\u8ABF\u7BC0
volume-controls-mute = \u6D88\u97F3
volume-controls-volume = \u97F3\u91CF
`},"ko-KR":{"context_menu.ftl":`context-menu-download-swf = .swf \uB2E4\uC6B4\uB85C\uB4DC
context-menu-copy-debug-info = \uB514\uBC84\uADF8 \uC815\uBCF4 \uBCF5\uC0AC
context-menu-open-save-manager = \uC800\uC7A5 \uAD00\uB9AC\uC790 \uC5F4\uAE30
context-menu-about-ruffle =
    { $flavor ->
        [extension] Ruffle \uD655\uC7A5 \uD504\uB85C\uADF8\uB7A8 \uC815\uBCF4 ({ $version })
       *[other] Ruffle \uC815\uBCF4 ({ $version })
    }
context-menu-hide = \uC774 \uBA54\uB274 \uC228\uAE30\uAE30
context-menu-exit-fullscreen = \uC804\uCCB4\uD654\uBA74 \uB098\uAC00\uAE30
context-menu-enter-fullscreen = \uC804\uCCB4\uD654\uBA74\uC73C\uB85C \uC5F4\uAE30
context-menu-volume-controls = \uC74C\uB7C9 \uC870\uC808
`,"messages.ftl":`message-cant-embed = Ruffle\uC774 \uC774 \uD398\uC774\uC9C0\uC5D0 \uD3EC\uD568\uB41C \uD50C\uB798\uC2DC\uB97C \uC2E4\uD589\uD560 \uC218 \uC5C6\uC5C8\uC2B5\uB2C8\uB2E4. \uBCC4\uB3C4\uC758 \uD0ED\uC5D0\uC11C \uD30C\uC77C\uC744 \uC5F4\uC5B4\uBD04\uC73C\uB85C\uC11C \uC774 \uBB38\uC81C\uB97C \uD574\uACB0\uD560 \uC218 \uC788\uC2B5\uB2C8\uB2E4.
panic-title = \uBB38\uC81C\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4 :(
more-info = \uCD94\uAC00 \uC815\uBCF4
run-anyway = \uADF8\uB798\uB3C4 \uC2E4\uD589\uD558\uAE30
continue = \uACC4\uC18D\uD558\uAE30
report-bug = \uBC84\uADF8 \uC81C\uBCF4
update-ruffle = Ruffle \uC5C5\uB370\uC774\uD2B8
ruffle-demo = \uC6F9 \uB370\uBAA8
ruffle-desktop = \uB370\uC2A4\uD06C\uD1B1 \uC560\uD50C\uB9AC\uCF00\uC774\uC158
ruffle-wiki = Ruffle \uC704\uD0A4 \uBCF4\uAE30
enable-hardware-acceleration = \uD558\uB4DC\uC6E8\uC5B4 \uAC00\uC18D\uC774 \uD65C\uC131\uD654\uB418\uC9C0 \uC54A\uC740 \uAC83 \uAC19\uC2B5\uB2C8\uB2E4. Ruffle\uC740 \uACC4\uC18D \uC791\uB3D9\uD558\uC9C0\uB9CC \uC2E4\uD589 \uC18D\uB3C4\uAC00 \uB9E4\uC6B0 \uB290\uB9B4 \uC218 \uC788\uC2B5\uB2C8\uB2E4. \uD558\uB4DC\uC6E8\uC5B4 \uAC00\uC18D\uC744 \uD65C\uC131\uD654\uD558\uB294 \uBC29\uBC95\uC744 \uC54C\uC544\uBCF4\uB824\uBA74 \uB2E4\uC74C \uB9C1\uD06C\uB97C \uCC38\uACE0\uD574\uBCF4\uC138\uC694.
view-error-details = \uC624\uB958 \uC138\uBD80 \uC815\uBCF4 \uBCF4\uAE30
open-in-new-tab = \uC0C8 \uD0ED\uC5D0\uC11C \uC5F4\uAE30
click-to-unmute = \uD074\uB9AD\uD558\uC5EC \uC74C\uC18C\uAC70 \uD574\uC81C
error-file-protocol =
    Ruffle\uC744 "file:" \uD504\uB85C\uD1A0\uCF5C\uC5D0\uC11C \uC2E4\uD589\uD558\uACE0 \uC788\uB294 \uAC83\uC73C\uB85C \uBCF4\uC785\uB2C8\uB2E4.
    \uBE0C\uB77C\uC6B0\uC800\uC5D0\uC11C\uB294 \uC774 \uD504\uB85C\uD1A0\uCF5C\uC744 \uBCF4\uC548\uC0C1\uC758 \uC774\uC720\uB85C \uB9CE\uC740 \uAE30\uB2A5\uC744 \uC791\uB3D9\uD558\uC9C0 \uC54A\uAC8C \uCC28\uB2E8\uD558\uBBC0\uB85C \uC774 \uBC29\uBC95\uC740 \uC791\uB3D9\uD558\uC9C0 \uC54A\uC2B5\uB2C8\uB2E4.
    \uB300\uC2E0, \uB85C\uCEEC \uC11C\uBC84\uB97C \uC9C1\uC811 \uC5F4\uC5B4\uC11C \uC124\uC815\uD558\uAC70\uB098 \uC6F9 \uB370\uBAA8 \uB610\uB294 \uB370\uC2A4\uD06C\uD1B1 \uC560\uD50C\uB9AC\uCF00\uC774\uC158\uC744 \uC0AC\uC6A9\uD558\uC2DC\uAE30 \uBC14\uB78D\uB2C8\uB2E4.
error-javascript-config =
    \uC798\uBABB\uB41C \uC790\uBC14\uC2A4\uD06C\uB9BD\uD2B8 \uC124\uC815\uC73C\uB85C \uC778\uD574 Ruffle\uC5D0\uC11C \uC911\uB300\uD55C \uBB38\uC81C\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4.
    \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uC778 \uACBD\uC6B0, \uC624\uB958 \uC138\uBD80\uC0AC\uD56D\uC744 \uD655\uC778\uD558\uC5EC \uC5B4\uB5A4 \uB9E4\uAC1C\uBCC0\uC218\uAC00 \uC798\uBABB\uB418\uC5C8\uB294\uC9C0 \uC54C\uC544\uBCF4\uC138\uC694.
    \uB610\uB294 Ruffle \uC704\uD0A4\uB97C \uD1B5\uD574 \uB3C4\uC6C0\uC744 \uBC1B\uC544 \uBCFC \uC218\uB3C4 \uC788\uC2B5\uB2C8\uB2E4.
error-wasm-not-found =
    Ruffle\uC774 ".wasm" \uD544\uC218 \uD30C\uC77C \uAD6C\uC131\uC694\uC18C\uB97C \uB85C\uB4DC\uD558\uC9C0 \uBABB\uD588\uC2B5\uB2C8\uB2E4.
    \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uB77C\uBA74 \uD30C\uC77C\uC774 \uC62C\uBC14\uB974\uAC8C \uC5C5\uB85C\uB4DC\uB418\uC5C8\uB294\uC9C0 \uD655\uC778\uD558\uC138\uC694.
    \uBB38\uC81C\uAC00 \uC9C0\uC18D\uB41C\uB2E4\uBA74 "publicPath" \uC635\uC158\uC744 \uC0AC\uC6A9\uD574\uC57C \uD560 \uC218\uB3C4 \uC788\uC2B5\uB2C8\uB2E4: Ruffle \uC704\uD0A4\uB97C \uCC38\uC870\uD558\uC5EC \uB3C4\uC6C0\uC744 \uBC1B\uC73C\uC138\uC694.
error-wasm-mime-type =
    Ruffle\uC774 \uCD08\uAE30\uD654\uB97C \uC2DC\uB3C4\uD558\uB294 \uB3D9\uC548 \uC911\uB300\uD55C \uBB38\uC81C\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4.
    \uC774 \uC6F9 \uC11C\uBC84\uB294 \uC62C\uBC14\uB978 MIME \uC720\uD615\uC758 ".wasm" \uD30C\uC77C\uC744 \uC81C\uACF5\uD558\uC9C0 \uC54A\uC2B5\uB2C8\uB2E4.
    \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uB77C\uBA74 Ruffle \uC704\uD0A4\uB97C \uD1B5\uD574 \uB3C4\uC6C0\uC744 \uBC1B\uC73C\uC138\uC694.
error-swf-fetch =
    Ruffle\uC774 \uD50C\uB798\uC2DC SWF \uD30C\uC77C\uC744 \uB85C\uB4DC\uD558\uB294 \uB370 \uC2E4\uD328\uD558\uC600\uC2B5\uB2C8\uB2E4.
    \uC774\uB294 \uC8FC\uB85C \uD30C\uC77C\uC774 \uB354 \uC774\uC0C1 \uC874\uC7AC\uD558\uC9C0 \uC54A\uC544 Ruffle\uC774 \uB85C\uB4DC\uD560 \uC218 \uC788\uB294 \uAC83\uC774 \uC5C6\uC744 \uAC00\uB2A5\uC131\uC774 \uB192\uC2B5\uB2C8\uB2E4.
    \uC6F9\uC0AC\uC774\uD2B8 \uAD00\uB9AC\uC790\uC5D0\uAC8C \uBB38\uC758\uD558\uC5EC \uB3C4\uC6C0\uC744 \uBC1B\uC544\uBCF4\uC138\uC694.
error-swf-cors =
    Ruffle\uC774 \uD50C\uB798\uC2DC SWF \uD30C\uC77C\uC744 \uB85C\uB4DC\uD558\uB294 \uB370 \uC2E4\uD328\uD558\uC600\uC2B5\uB2C8\uB2E4.
    CORS \uC815\uCC45\uC5D0 \uC758\uD574 \uB370\uC774\uD130 \uAC00\uC838\uC624\uAE30\uC5D0 \uB300\uD55C \uC561\uC138\uC2A4\uAC00 \uCC28\uB2E8\uB418\uC5C8\uC744 \uC218 \uC788\uC2B5\uB2C8\uB2E4.
    \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uB77C\uBA74 Ruffle \uC704\uD0A4\uB97C \uCC38\uC870\uD558\uC5EC \uB3C4\uC6C0\uC744 \uBC1B\uC544\uBCFC \uC218 \uC788\uC2B5\uB2C8\uB2E4.
error-wasm-cors =
    Ruffle\uC774 ".wasm" \uD544\uC218 \uD30C\uC77C \uAD6C\uC131\uC694\uC18C\uB97C \uB85C\uB4DC\uD558\uC9C0 \uBABB\uD588\uC2B5\uB2C8\uB2E4.
    CORS \uC815\uCC45\uC5D0 \uC758\uD574 \uB370\uC774\uD130 \uAC00\uC838\uC624\uAE30\uC5D0 \uB300\uD55C \uC561\uC138\uC2A4\uAC00 \uCC28\uB2E8\uB418\uC5C8\uC744 \uC218 \uC788\uC2B5\uB2C8\uB2E4.
    \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uB77C\uBA74 Ruffle \uC704\uD0A4\uB97C \uCC38\uC870\uD558\uC5EC \uB3C4\uC6C0\uC744 \uBC1B\uC544\uBCFC \uC218 \uC788\uC2B5\uB2C8\uB2E4.
error-wasm-invalid =
    Ruffle\uC774 \uCD08\uAE30\uD654\uB97C \uC2DC\uB3C4\uD558\uB294 \uB3D9\uC548 \uC911\uB300\uD55C \uBB38\uC81C\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4.
    \uC774 \uD398\uC774\uC9C0\uC5D0 Ruffle\uC744 \uC2E4\uD589\uD558\uAE30 \uC704\uD55C \uD30C\uC77C\uC774 \uB204\uB77D\uB418\uC5C8\uAC70\uB098 \uC798\uBABB\uB41C \uAC83 \uAC19\uC2B5\uB2C8\uB2E4.
    \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uB77C\uBA74 Ruffle \uC704\uD0A4\uB97C \uCC38\uC870\uD558\uC5EC \uB3C4\uC6C0\uC744 \uBC1B\uC544\uBCFC \uC218 \uC788\uC2B5\uB2C8\uB2E4.
error-wasm-download =
    Ruffle\uC774 \uCD08\uAE30\uD654\uB97C \uC2DC\uB3C4\uD558\uB294 \uB3D9\uC548 \uC911\uB300\uD55C \uBB38\uC81C\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4.
    \uC774 \uBB38\uC81C\uB294 \uB54C\uB54C\uB85C \uBC14\uB85C \uD574\uACB0\uB420 \uC218 \uC788\uC73C\uBBC0\uB85C \uD398\uC774\uC9C0\uB97C \uC0C8\uB85C\uACE0\uCE68\uD558\uC5EC \uB2E4\uC2DC \uC2DC\uB3C4\uD574\uBCF4\uC138\uC694.
    \uADF8\uB798\uB3C4 \uBB38\uC81C\uAC00 \uC9C0\uC18D\uB41C\uB2E4\uBA74, \uC6F9\uC0AC\uC774\uD2B8 \uAD00\uB9AC\uC790\uC5D0\uAC8C \uBB38\uC758\uD574\uC8FC\uC138\uC694.
error-wasm-disabled-on-edge =
    Ruffle\uC774 ".wasm" \uD544\uC218 \uD30C\uC77C \uAD6C\uC131\uC694\uC18C\uB97C \uB85C\uB4DC\uD558\uC9C0 \uBABB\uD588\uC2B5\uB2C8\uB2E4.
    \uC774\uB97C \uD574\uACB0\uD558\uB824\uBA74 \uBE0C\uB77C\uC6B0\uC800 \uC124\uC815\uC5D0\uC11C "\uAC1C\uC778 \uC815\uBCF4, \uAC80\uC0C9 \uBC0F \uC11C\uBE44\uC2A4"\uB97C \uD074\uB9AD\uD55C \uD6C4, \uD558\uB2E8\uC73C\uB85C \uC2A4\uD06C\uB864\uD558\uC5EC "\uC6F9\uC5D0\uC11C \uBCF4\uC548 \uAC15\uD654" \uAE30\uB2A5\uC744 \uAEBC\uC57C \uD569\uB2C8\uB2E4.
    \uC774\uB294 \uD544\uC694\uD55C ".wasm" \uD30C\uC77C\uC744 \uBE0C\uB77C\uC6B0\uC800\uC5D0\uC11C \uB85C\uB4DC\uD560 \uC218 \uC788\uB3C4\uB85D \uD5C8\uC6A9\uD569\uB2C8\uB2E4.
    \uC774 \uBB38\uC81C\uAC00 \uC9C0\uC18D\uB420 \uACBD\uC6B0 \uB2E4\uB978 \uBE0C\uB77C\uC6B0\uC800\uB97C \uC0AC\uC6A9\uD574\uC57C \uD560 \uC218 \uC788\uC2B5\uB2C8\uB2E4.
error-javascript-conflict =
    Ruffle\uC774 \uCD08\uAE30\uD654\uB97C \uC2DC\uB3C4\uD558\uB294 \uB3D9\uC548 \uC911\uB300\uD55C \uBB38\uC81C\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4.
    \uC774 \uD398\uC774\uC9C0\uC5D0\uC11C \uC0AC\uC6A9\uB418\uB294 \uC790\uBC14\uC2A4\uD06C\uB9BD\uD2B8 \uCF54\uB4DC\uAC00 Ruffle\uACFC \uCDA9\uB3CC\uD558\uB294 \uAC83\uC73C\uB85C \uBCF4\uC785\uB2C8\uB2E4.
    \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uB77C\uBA74 \uBE48 \uD398\uC774\uC9C0\uC5D0\uC11C \uD30C\uC77C\uC744 \uB85C\uB4DC\uD574\uBCF4\uC138\uC694.
error-javascript-conflict-outdated = \uB610\uD55C Ruffle\uC758 \uCD5C\uC2E0 \uBC84\uC804\uC744 \uC5C5\uB85C\uB4DC\uD558\uB294 \uAC83\uC744 \uC2DC\uB3C4\uD558\uC5EC \uBB38\uC81C\uB97C \uC6B0\uD68C\uD574\uBCFC \uC218 \uC788\uC2B5\uB2C8\uB2E4. (\uD604\uC7AC \uBE4C\uB4DC\uAC00 \uC624\uB798\uB418\uC5C8\uC2B5\uB2C8\uB2E4: { $buildDate }).
error-csp-conflict =
    Ruffle\uC774 \uCD08\uAE30\uD654\uB97C \uC2DC\uB3C4\uD558\uB294 \uB3D9\uC548 \uC911\uB300\uD55C \uBB38\uC81C\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4.
    \uC774 \uC6F9 \uC11C\uBC84\uC758 CSP(Content Security Policy) \uC815\uCC45\uC774 ".wasm" \uD544\uC218 \uAD6C\uC131\uC694\uC18C\uB97C \uC2E4\uD589\uD558\uB294 \uAC83\uC744 \uD5C8\uC6A9\uD558\uC9C0 \uC54A\uC2B5\uB2C8\uB2E4.
    \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uB77C\uBA74 Ruffle \uC704\uD0A4\uB97C \uCC38\uC870\uD558\uC5EC \uB3C4\uC6C0\uC744 \uBC1B\uC544\uBCFC \uC218 \uC788\uC2B5\uB2C8\uB2E4.
error-unknown =
    Ruffle\uC774 \uD50C\uB798\uC2DC \uCF58\uD150\uCE20\uB97C \uD45C\uC2DC\uD558\uB824\uACE0 \uC2DC\uB3C4\uD558\uB294 \uB3D9\uC548 \uC911\uB300\uD55C \uBB38\uC81C\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4.
    { $outdated ->
        [true] \uB9CC\uC57D \uB2F9\uC2E0\uC774 \uC11C\uBC84 \uAD00\uB9AC\uC790\uB77C\uBA74, Ruffle\uC758 \uCD5C\uC2E0 \uBC84\uC804\uC744 \uC5C5\uB85C\uB4DC\uD558\uC5EC \uB2E4\uC2DC \uC2DC\uB3C4\uD574\uBCF4\uC138\uC694. (\uD604\uC7AC \uBE4C\uB4DC\uAC00 \uC624\uB798\uB418\uC5C8\uC2B5\uB2C8\uB2E4: { $buildDate }).
       *[false] \uC774\uB7F0 \uD604\uC0C1\uC774 \uBC1C\uC0DD\uD574\uC11C\uB294 \uC548\uB418\uBBC0\uB85C, \uBC84\uADF8\uB97C \uC81C\uBCF4\uD574\uC8FC\uC2E0\uB2E4\uBA74 \uAC10\uC0AC\uD558\uACA0\uC2B5\uB2C8\uB2E4!
    }
`,"save-manager.ftl":`save-delete-prompt = \uC815\uB9D0\uB85C \uC774 \uC138\uC774\uBE0C \uD30C\uC77C\uC744 \uC0AD\uC81C\uD558\uC2DC\uACA0\uC2B5\uB2C8\uAE4C?
save-reload-prompt =
    \b\uC774 \uD30C\uC77C\uC744 \uC7A0\uC7AC\uC801\uC778 \uCDA9\uB3CC \uC5C6\uC774 { $action ->
        [delete] \uC0AD\uC81C
       *[replace] \uAD50\uCCB4
    }\uD558\uB824\uBA74 \uCF58\uD150\uCE20\uB97C \uB2E4\uC2DC \uB85C\uB4DC\uD574\uC57C \uD569\uB2C8\uB2E4. \uADF8\uB798\uB3C4 \uACC4\uC18D\uD558\uC2DC\uACA0\uC2B5\uB2C8\uAE4C?
save-download = \uB2E4\uC6B4\uB85C\uB4DC
save-replace = \uAD50\uCCB4
save-delete = \uC0AD\uC81C
save-backup-all = \uBAA8\uB4E0 \uC800\uC7A5 \uD30C\uC77C \uB2E4\uC6B4\uB85C\uB4DC
`,"volume-controls.ftl":`volume-controls = \uC74C\uB7C9 \uC870\uC808
volume-controls-mute = \uC74C\uC18C\uAC70
volume-controls-volume = \uC74C\uB7C9
`},"nl-NL":{"context_menu.ftl":`context-menu-download-swf = .swf downloaden
context-menu-copy-debug-info = Kopieer debuginformatie
context-menu-open-save-manager = Open opgeslagen-data-manager
context-menu-about-ruffle =
    { $flavor ->
        [extension] Over Ruffle Uitbreiding ({ $version })
       *[other] Over Ruffle ({ $version })
    }
context-menu-hide = Verberg dit menu
context-menu-exit-fullscreen = Verlaat volledig scherm
context-menu-enter-fullscreen = Naar volledig scherm
`,"messages.ftl":`message-cant-embed =
    Ruffle kon de Flash-inhoud op de pagina niet draaien.
    Je kan proberen het bestand in een apart tabblad te openen, om hier omheen te werken.
panic-title = Er ging iets mis :(
more-info = Meer informatie
run-anyway = Toch starten
continue = Doorgaan
report-bug = Bug rapporteren
update-ruffle = Ruffle updaten
ruffle-demo = Web Demo
ruffle-desktop = Desktopapplicatie
ruffle-wiki = Bekijk de Ruffle Wiki
view-error-details = Foutdetails tonen
open-in-new-tab = Openen in een nieuw tabblad
click-to-unmute = Klik om te ontdempen
error-file-protocol =
    Het lijkt erop dat je Ruffle gebruikt met het "file" protocol.
    De meeste browsers blokkeren dit om veiligheidsredenen, waardoor het niet werkt.
    In plaats hiervan raden we aan om een lokale server te draaien, de web demo te gebruiken, of de desktopapplicatie.
error-javascript-config =
    Ruffle heeft een groot probleem ondervonden vanwege een onjuiste JavaScript configuratie.
    Als je de serverbeheerder bent, kijk dan naar de foutdetails om te zien wat er verkeerd is.
    Je kan ook in de Ruffle wiki kijken voor hulp.
error-wasm-not-found =
    Ruffle kon het vereiste ".wasm" bestandscomponent niet laden.
    Als je de serverbeheerder bent, controleer dan of het bestaand juist is ge\xFCpload.
    Mocht het probleem blijven voordoen, moet je misschien de "publicPath" instelling gebruiken: zie ook de Ruffle wiki voor hulp.
error-wasm-mime-type =
    Ruffle heeft een groot probleem ondervonden tijdens het initialiseren.
    Deze webserver serveert ".wasm" bestanden niet met het juiste MIME type.
    Als je de serverbeheerder bent, kijk dan in de Ruffle wiki voor hulp.
error-swf-fetch =
    Ruffle kon het Flash SWF bestand niet inladen.
    De meest waarschijnlijke reden is dat het bestand niet langer bestaat, en er dus niets is om in te laden.
    Probeer contact op te nemen met de websitebeheerder voor hulp.
error-swf-cors =
    Ruffle kon het Flash SWD bestand niet inladen.
    Toegang is waarschijnlijk geblokeerd door het CORS beleid.
    Als je de serverbeheerder bent, kijk dan in de Ruffle wiki voor hulp.
error-wasm-cors =
    Ruffle kon het vereiste ".wasm" bestandscomponent niet laden.
    Toegang is waarschijnlijk geblokeerd door het CORS beleid.
    Als je de serverbeheerder bent, kijk dan in de Ruffle wiki voor hulp.
error-wasm-invalid =
    Ruffle heeft een groot probleem ondervonden tijdens het initialiseren.
    Het lijkt erop dat de Ruffle bestanden ontbreken of ongeldig zijn.
    Als je de serverbeheerder bent, kijk dan in de Ruffle wiki voor hulp.
error-wasm-download =
    Ruffle heeft een groot probleem ondervonden tijdens het initialiseren.
    Dit lost zichzelf vaak op als je de bladzijde opnieuw inlaadt.
    Zo niet, neem dan contact op met de websitebeheerder.
error-wasm-disabled-on-edge =
    Ruffle kon het vereiste ".wasm" bestandscomponent niet laden.
    Om dit op te lossen, ga naar je browserinstellingen, klik op "Privacy, zoeken en diensten", scroll omlaag, en schakel "Verbeter je veiligheid op he web" uit.
    Dan kan je browser wel de vereiste ".wasm" bestanden inladen.
    Als het probleem zich blijft voordoen, moet je misschien een andere browser gebruiken.
error-javascript-conflict =
    Ruffle heeft een groot probleem ondervonden tijdens het initialiseren.
    Het lijkt erop dat deze pagina JavaScript code gebruikt die conflicteert met Ruffle.
    Als je de serverbeheerder bent, raden we aan om het bestand op een lege pagina te proberen in te laden.
error-javascript-conflict-outdated = Je kan ook proberen een nieuwe versie van Ruffle te installeren, om om het probleem heen te werken (huidige versie is oud: { $buildDate }).
error-csp-conflict =
    Ruffle heeft een groot probleem ondervonden tijdens het initialiseren.
    Het CSP-beleid staat niet toe dat het vereiste ".wasm" component kan draaien.
    Als je de serverbeheerder bent, kijk dan in de Ruffle wiki voor hulp.
error-unknown =
    Ruffle heeft een groot probleem onderbonden tijdens het weergeven van deze Flash-inhoud.
    { $outdated ->
        [true] Als je de serverbeheerder bent, upload dan een nieuwe versie van Ruffle (huidige versie is oud: { $buildDate }).
       *[false] Dit hoort niet te gebeuren, dus we stellen het op prijs als je de fout aan ons rapporteert!
    }
`,"save-manager.ftl":`save-delete-prompt = Weet je zeker dat je deze opgeslagen data wilt verwijderen?
save-reload-prompt =
    De enige manier om deze opgeslagen data te { $action ->
        [delete] verwijderen
       *[replace] vervangen
    } zonder potenti\xEBle problemen is door de inhoud opnieuw te laden. Toch doorgaan?
save-download = Downloaden
save-replace = Vervangen
save-delete = Verwijderen
save-backup-all = Download alle opgeslagen data
`,"volume-controls.ftl":""},"pl-PL":{"context_menu.ftl":`context-menu-download-swf = Pobierz .swf
context-menu-copy-debug-info = Kopiuj informacje debugowania
context-menu-open-save-manager = Otw\xF3rz Menad\u017Cer Zapis\xF3w
context-menu-about-ruffle =
    { $flavor ->
        [extension] O Rozszerzeniu Ruffle ({ $version })
       *[other] O Ruffle ({ $version })
    }
context-menu-hide = Ukryj to menu
context-menu-exit-fullscreen = Zamknij pe\u0142ny ekran
context-menu-enter-fullscreen = Pe\u0142ny ekran
context-menu-volume-controls = Sterowanie g\u0142o\u015Bno\u015Bci\u0105
`,"messages.ftl":`message-cant-embed =
    Ruffle nie by\u0142o w stanie uruchomi\u0107 zawarto\u015Bci Flash w tej stronie.
    Mo\u017Cesz spr\xF3bowa\u0107 otworzy\u0107 plik w nowej karcie, aby unikn\u0105\u0107 tego problemu.
panic-title = Co\u015B posz\u0142o nie tak :(
more-info = Wi\u0119cej informacji
run-anyway = Uruchom mimo tego
continue = Kontynuuj
report-bug = Zg\u0142o\u015B b\u0142\u0105d
update-ruffle = Zaktualizuj Ruffle
ruffle-desktop = Aplikacja na komputer
ruffle-wiki = Zobacz Wiki Ruffle
enable-hardware-acceleration = Wygl\u0105da na to, \u017Ce akceleracja sprz\u0119towa nie jest w\u0142\u0105czona. Chocia\u017C Ruffle mo\u017Ce dzia\u0142a\u0107, mo\u017Ce by\u0107 nieproporcjonalnie wolna. Mo\u017Cesz dowiedzie\u0107 si\u0119, jak w\u0142\u0105czy\u0107 akceleracj\u0119 sprz\u0119tow\u0105, pod\u0105\u017Caj\u0105c za tym linkiem.
view-error-details = Zobacz szczeg\xF3\u0142y b\u0142\u0119du
open-in-new-tab = Otw\xF3rz w nowej karcie
click-to-unmute = Kliknij aby wy\u0142\u0105czy\u0107 wyciszenie
error-file-protocol =
    Wygl\u0105da na to, \u017Ce u\u017Cywasz Ruffle w protokole "plik:".
    To nie dzia\u0142a poniewa\u017C przegl\u0105darka blokuje wiele funkcji przed dzia\u0142aniem ze wzgl\u0119d\xF3w bezpiecze\u0144stwa.
    Zamiast tego zapraszamy do konfiguracji serwera lokalnego lub u\u017Cycia aplikacji demo lub desktopowej.
error-javascript-config =
    Ruffle napotka\u0142 powa\u017Cny problem z powodu nieprawid\u0142owej konfiguracji JavaScript.
    Je\u015Bli jeste\u015B administratorem serwera, prosimy o sprawdzenie szczeg\xF3\u0142\xF3w b\u0142\u0119du, aby dowiedzie\u0107 si\u0119, kt\xF3ry parametr jest b\u0142\u0119dny.
    Mo\u017Cesz r\xF3wnie\u017C zapozna\u0107 si\u0119 z wiki Ruffle po pomoc.
error-wasm-not-found =
    Ruffle nie uda\u0142o si\u0119 za\u0142adowa\u0107 wymaganego komponentu pliku ".wasm".
    Je\u015Bli jeste\u015B administratorem serwera, upewnij si\u0119, \u017Ce plik zosta\u0142 poprawnie przes\u0142any.
    Je\u015Bli problem b\u0119dzie si\u0119 powtarza\u0142, by\u0107 mo\u017Ce b\u0119dziesz musia\u0142 u\u017Cy\u0107 ustawienia "publicPath": zapoznaj si\u0119 z wiki Ruffle aby uzyska\u0107 pomoc.
error-wasm-mime-type =
    Ruffle napotka\u0142 powa\u017Cny problem podczas pr\xF3by zainicjowania.
    Ten serwer internetowy nie obs\u0142uguje ". asm" pliki z poprawnym typem MIME.
    Je\u015Bli jeste\u015B administratorem serwera, zapoznaj si\u0119 z wiki Ruffle aby uzyska\u0107 pomoc.
error-swf-fetch =
    Ruffle nie uda\u0142o si\u0119 za\u0142adowa\u0107 pliku Flash SWF.
    Najbardziej prawdopodobnym powodem jest to, \u017Ce plik ju\u017C nie istnieje, wi\u0119c Ruffle nie ma nic do za\u0142adowania.
    Spr\xF3buj skontaktowa\u0107 si\u0119 z administratorem witryny, aby uzyska\u0107 pomoc.
error-swf-cors =
    Ruffle nie uda\u0142o si\u0119 za\u0142adowa\u0107 pliku Flash SWF.
    Dost\u0119p do pobierania zosta\u0142 prawdopodobnie zablokowany przez polityk\u0119 CORS.
    Je\u015Bli jeste\u015B administratorem serwera, prosimy o pomoc z wiki Ruffle.
error-wasm-cors =
    Ruffle nie uda\u0142o si\u0119 za\u0142adowa\u0107 wymaganego komponentu pliku ".wasm".
    Dost\u0119p do pobierania zosta\u0142 prawdopodobnie zablokowany przez polityk\u0119 CORS.
    Je\u015Bli jeste\u015B administratorem serwera, prosimy o pomoc z wiki Ruffle.
error-wasm-invalid =
    Ruffle napotka\u0142 powa\u017Cny problem podczas pr\xF3by zainicjowania.
    Wygl\u0105da na to, \u017Ce ta strona ma brakuj\u0105ce lub nieprawid\u0142owe pliki do uruchomienia Ruffle.
    Je\u015Bli jeste\u015B administratorem serwera, prosimy o pomoc z wiki Ruffle.
error-wasm-download =
    Ruffle napotka\u0142 powa\u017Cny problem podczas pr\xF3by zainicjowania.
    Mo\u017Ce to cz\u0119sto rozwi\u0105za\u0107 siebie, wi\u0119c mo\u017Cesz spr\xF3bowa\u0107 od\u015Bwie\u017Cy\u0107 stron\u0119.
    W przeciwnym razie skontaktuj si\u0119 z administratorem witryny.
error-wasm-disabled-on-edge =
    Ruffle nie uda\u0142o si\u0119 za\u0142adowa\u0107 wymaganego komponentu pliku ".wasm".
    Aby to naprawi\u0107, spr\xF3buj otworzy\u0107 ustawienia przegl\u0105darki, klikaj\u0105c "Prywatno\u015B\u0107, wyszukiwanie i us\u0142ugi", przewijaj\u0105c w d\xF3\u0142 i wy\u0142\u0105czaj\u0105c "Zwi\u0119ksz bezpiecze\u0144stwo w sieci".
    Pozwoli to przegl\u0105darce za\u0142adowa\u0107 wymagane pliki ".wasm".
    Je\u015Bli problem b\u0119dzie si\u0119 powtarza\u0142, by\u0107 mo\u017Ce b\u0119dziesz musia\u0142 u\u017Cy\u0107 innej przegl\u0105darki.
error-javascript-conflict =
    Ruffle napotka\u0142 powa\u017Cny problem podczas pr\xF3by zainicjowania.
    Wygl\u0105da na to, \u017Ce ta strona u\u017Cywa kodu JavaScript, kt\xF3ry koliduje z Ruffle.
    Je\u015Bli jeste\u015B administratorem serwera, zapraszamy Ci\u0119 do \u0142adowania pliku na pustej stronie.
error-javascript-conflict-outdated = Mo\u017Cesz r\xF3wnie\u017C spr\xF3bowa\u0107 przes\u0142a\u0107 nowsz\u0105 wersj\u0119 Ruffle, kt\xF3ra mo\u017Ce omin\u0105\u0107 problem (obecna wersja jest przestarza\u0142a: { $buildDate }).
error-csp-conflict =
    Ruffle napotka\u0142 powa\u017Cny problem podczas pr\xF3by zainicjowania.
    Polityka bezpiecze\u0144stwa zawarto\u015Bci tego serwera nie zezwala na wymagany ". wasm" komponent do uruchomienia.
    Je\u015Bli jeste\u015B administratorem serwera, zapoznaj si\u0119 z wiki Ruffle po pomoc.
error-unknown =
    Ruffle napotka\u0142 powa\u017Cny problem podczas pr\xF3by wy\u015Bwietlenia tej zawarto\u015Bci Flash.
    { $outdated ->
        [true] Je\u015Bli jeste\u015B administratorem serwera, spr\xF3buj przes\u0142a\u0107 nowsz\u0105 wersj\u0119 Ruffle (obecna wersja jest przestarza\u0142a: { $buildDate }).
       *[false] To nie powinno si\u0119 wydarzy\u0107, wi\u0119c byliby\u015Bmy wdzi\u0119czni, gdyby\u015B m\xF3g\u0142 zg\u0142osi\u0107 b\u0142\u0105d!
    }
`,"save-manager.ftl":`save-delete-prompt = Czy na pewno chcesz skasowa\u0107 ten plik zapisu?
save-reload-prompt =
    Jedyn\u0105 opcj\u0105, aby { $action ->
        [delete] usun\u0105\u0107
       *[replace] zamieni\u0107
    } ten plik zapisu bez potencjalnych konflikt\xF3w jest prze\u0142adowanie zawarto\u015Bci. Czy chcesz kontynuowa\u0107?
save-download = Pobierz
save-replace = Zamie\u0144
save-delete = Usu\u0144
save-backup-all = Pobierz wszystkie pliki zapisu
`,"volume-controls.ftl":`volume-controls = Sterowanie g\u0142o\u015Bno\u015Bci\u0105
volume-controls-mute = Wycisz
volume-controls-volume = G\u0142o\u015Bno\u015B\u0107
`},"pt-BR":{"context_menu.ftl":`context-menu-download-swf = Baixar .swf
context-menu-copy-debug-info = Copiar informa\xE7\xE3o de depura\xE7\xE3o
context-menu-open-save-manager = Abrir o Gerenciador de Salvamento
context-menu-about-ruffle =
    { $flavor ->
        [extension] Sobre a extens\xE3o do Ruffle ({ $version })
       *[other] Sobre o Ruffle ({ $version })
    }
context-menu-hide = Esconder este menu
context-menu-exit-fullscreen = Sair da tela cheia
context-menu-enter-fullscreen = Entrar em tela cheia
`,"messages.ftl":`message-cant-embed =
    Ruffle n\xE3o conseguiu executar o Flash incorporado nesta p\xE1gina.
    Voc\xEA pode tentar abrir o arquivo em uma guia separada para evitar esse problema.
panic-title = Algo deu errado :(
more-info = Mais informa\xE7\xE3o
run-anyway = Executar mesmo assim
continue = Continuar
report-bug = Reportar Bug
update-ruffle = Atualizar Ruffle
ruffle-demo = Demo Web
ruffle-desktop = Aplicativo de Desktop
ruffle-wiki = Ver Wiki do Ruffle
view-error-details = Ver detalhes do erro
open-in-new-tab = Abrir em uma nova guia
click-to-unmute = Clique para ativar o som
error-file-protocol =
    Parece que voc\xEA est\xE1 executando o Ruffle no protocolo "file:".
    Isto n\xE3o funciona como navegadores bloqueiam muitos recursos de funcionar por raz\xF5es de seguran\xE7a.
    Ao inv\xE9s disso, convidamos voc\xEA a configurar um servidor local ou a usar a demonstra\xE7\xE3o da web, ou o aplicativo de desktop.
error-javascript-config =
    O Ruffle encontrou um grande problema devido a uma configura\xE7\xE3o incorreta do JavaScript.
    Se voc\xEA for o administrador do servidor, convidamos voc\xEA a verificar os detalhes do erro para descobrir qual par\xE2metro est\xE1 com falha.
    Voc\xEA tamb\xE9m pode consultar o wiki do Ruffle para obter ajuda.
error-wasm-not-found =
    Ruffle falhou ao carregar o componente de arquivo ".wasm" necess\xE1rio.
    Se voc\xEA \xE9 o administrador do servidor, por favor, certifique-se de que o arquivo foi carregado corretamente.
    Se o problema persistir, voc\xEA pode precisar usar a configura\xE7\xE3o "publicPath": por favor consulte a wiki do Ruffle para obter ajuda.
error-wasm-mime-type =
    Ruffle encontrou um grande problema ao tentar inicializar.
    Este servidor de web n\xE3o est\xE1 servindo ".wasm" arquivos com o tipo MIME correto.
    Se voc\xEA \xE9 o administrador do servidor, por favor consulte o wiki do Ruffle para obter ajuda.
error-swf-fetch =
    Ruffle falhou ao carregar o arquivo Flash SWF.
    A raz\xE3o prov\xE1vel \xE9 que o arquivo n\xE3o existe mais, ent\xE3o n\xE3o h\xE1 nada para o Ruffle carregar.
    Tente contatar o administrador do site para obter ajuda.
error-swf-cors =
    Ruffle falhou ao carregar o arquivo Flash SWF.
    O acesso para fetch provavelmente foi bloqueado pela pol\xEDtica CORS.
    Se voc\xEA for o administrador do servidor, consulte o wiki do Ruffle para obter ajuda.
error-wasm-cors =
    Ruffle falhou ao carregar o componente de arquivo ".wasm" necess\xE1rio.
    O acesso para fetch foi provavelmente bloqueado pela pol\xEDtica CORS.
    Se voc\xEA \xE9 o administrador do servidor, por favor consulte a wiki do Ruffle para obter ajuda.
error-wasm-invalid =
    Ruffle encontrou um grande problema ao tentar inicializar.
    Parece que esta p\xE1gina tem arquivos ausentes ou inv\xE1lidos para executar o Ruffle.
    Se voc\xEA for o administrador do servidor, consulte o wiki do Ruffle para obter ajuda.
error-wasm-download =
    O Ruffle encontrou um grande problema ao tentar inicializar.
    Muitas vezes isso pode se resolver sozinho, ent\xE3o voc\xEA pode tentar recarregar a p\xE1gina.
    Caso contr\xE1rio, contate o administrador do site.
error-wasm-disabled-on-edge =
    O Ruffle falhou ao carregar o componente de arquivo ".wasm" necess\xE1rio.
    Para corrigir isso, tente abrir configura\xE7\xF5es do seu navegador, clicando em "Privacidade, pesquisa e servi\xE7os", rolando para baixo e desativando "Melhore sua seguran\xE7a na web".
    Isso permitir\xE1 que seu navegador carregue os arquivos ".wasm" necess\xE1rios.
    Se o problema persistir, talvez seja necess\xE1rio usar um navegador diferente.
error-javascript-conflict =
    Ruffle encontrou um grande problema ao tentar inicializar.
    Parece que esta p\xE1gina usa c\xF3digo JavaScript que entra em conflito com o Ruffle.
    Se voc\xEA for o administrador do servidor, convidamos voc\xEA a tentar carregar o arquivo em uma p\xE1gina em branco.
error-javascript-conflict-outdated = Voc\xEA tamb\xE9m pode tentar fazer o upload de uma vers\xE3o mais recente do Ruffle que pode contornar o problema (a compila\xE7\xE3o atual est\xE1 desatualizada: { $buildDate }).
error-csp-conflict =
    Ruffle encontrou um grande problema ao tentar inicializar.
    A pol\xEDtica de seguran\xE7a de conte\xFAdo deste servidor da web n\xE3o permite a execu\xE7\xE3o do componente ".wasm" necess\xE1rio.
    Se voc\xEA for o administrador do servidor, consulte o wiki do Ruffle para obter ajuda.
error-unknown =
    O Ruffle encontrou um grande problema enquanto tentava exibir este conte\xFAdo em Flash.
    { $outdated ->
        [true] Se voc\xEA \xE9 o administrador do servidor, por favor tente fazer o upload de uma vers\xE3o mais recente do Ruffle (a compila\xE7\xE3o atual est\xE1 desatualizada: { $buildDate }).
       *[false] Isso n\xE3o deveria acontecer, ent\xE3o apreciar\xEDamos muito se voc\xEA pudesse arquivar um bug!
    }
`,"save-manager.ftl":`save-delete-prompt = Tem certeza que deseja excluir este arquivo de salvamento?
save-reload-prompt =
    A \xFAnica maneira de { $action ->
        [delete] excluir
       *[replace] substituir
    } este arquivo sem potencial conflito \xE9 recarregar este conte\xFAdo. Deseja continuar mesmo assim?
save-download = Baixar
save-replace = Substituir
save-delete = Excluir
save-backup-all = Baixar todos os arquivos de salvamento
`,"volume-controls.ftl":""},"pt-PT":{"context_menu.ftl":`context-menu-download-swf = Descarga.swf
context-menu-copy-debug-info = Copiar informa\xE7\xF5es de depura\xE7\xE3o
context-menu-open-save-manager = Abrir Gestor de Grava\xE7\xF5es
context-menu-about-ruffle =
    { $flavor ->
        [extension] Sobre a extens\xE3o do Ruffle ({ $version })
       *[other] Sobre o Ruffle ({ $version })
    }
context-menu-hide = Esconder este menu
context-menu-exit-fullscreen = Fechar Ecr\xE3 Inteiro
context-menu-enter-fullscreen = Abrir Ecr\xE3 Inteiro
context-menu-volume-controls = Controlos de volume
`,"messages.ftl":`message-cant-embed =
    O Ruffle n\xE3o conseguiu abrir o Flash integrado nesta p\xE1gina.
    Para tentar resolver o problema, pode abrir o ficheiro num novo separador.
panic-title = Algo correu mal :(
more-info = Mais informa\xE7\xF5es
run-anyway = Executar mesmo assim
continue = Continuar
report-bug = Reportar falha
update-ruffle = Atualizar o Ruffle
ruffle-demo = Demonstra\xE7\xE3o na Web
ruffle-desktop = Aplica\xE7\xE3o para Desktop
ruffle-wiki = Ver a Wiki do Ruffle
enable-hardware-acceleration = Parece que a acelera\xE7\xE3o de hardware n\xE3o est\xE1 ativada. Mesmo que o Ruffle funcione, pode estar excessivamente lento. Descubra como ativar a acelera\xE7\xE3o de hardware seguindo este link.
view-error-details = Ver detalhes do erro
open-in-new-tab = Abrir num novo separador
click-to-unmute = Clique para ativar o som
error-file-protocol =
    Parece que executa o Ruffle no protocolo "file:".
    Isto n\xE3o funciona, j\xE1 que os navegadores bloqueiam muitas funcionalidades por raz\xF5es de seguran\xE7a.
    Em vez disto, recomendados configurar um servidor local ou usar a demonstra\xE7\xE3o na web, ou a aplica\xE7\xE3o para desktop.
error-javascript-config =
    O Ruffle encontrou um problema maior devido a uma configura\xE7\xE3o de JavaScript incorreta.
    Se \xE9 o administrador do servidor, convidamo-lo a verificar os detalhes do erro para descobrir o par\xE2metro problem\xE1tico.
    Pode ainda consultar a wiki do Ruffle para obter ajuda.
error-wasm-not-found =
    O Ruffle falhou ao carregar o componente de ficheiro ".wasm" necess\xE1rio.
    Se \xE9 o administrador do servidor, por favor certifique-se de que o ficheiro foi devidamente carregado.
    Se o problema persistir, poder\xE1 querer usar a configura\xE7\xE3o "publicPath": consulte a wiki do Ruffle para obter ajuda.
error-wasm-mime-type =
    O Ruffle encontrou um problema maior ao tentar inicializar.
    Este servidor de web n\xE3o suporta ficheiros ".wasm" com o tipo MIME correto.
    Se \xE9 o administrador do servidor, por favor consulte o wiki do Ruffle para obter ajuda.
error-swf-fetch =
    Ruffle falhou ao carregar o arquivo SWF do Flash
    A raz\xE3o mais prov\xE1vel \xE9 que o arquivo n\xE3o existe mais, ent\xE3o n\xE3o h\xE1 nada para o Ruffle carregar.
    Tente contactar o administrador do site para obter ajuda.
error-swf-cors =
    O Ruffle falhou ao carregar o ficheiro Flash SWF.
    Acesso a buscar foi provavelmente bloqueado pela pol\xEDtica de CORS.
    Se \xE9 o administrador do servidor, por favor consulte a wiki do Ruffle para obter ajuda.
error-wasm-cors =
    O Ruffle falhou ao carregar o componente de ficheiro ".wasm" necess\xE1rio.
    O acesso a buscar foi provavelmente bloqueado pela pol\xEDtica CORS.
    Se \xE9 o administrador do servidor, por favor consulte a wiki do Ruffle para obter ajuda.
error-wasm-invalid =
    Ruffle encontrou um grande problema ao tentar inicializar.
    Parece que esta p\xE1gina est\xE1 ausente ou arquivos inv\xE1lidos para executar o Ruffle.
    Se voc\xEA \xE9 o administrador do servidor, por favor consulte a wiki do Ruffle para obter ajuda.
error-wasm-download =
    O Ruffle encontrou um problema maior ao tentar inicializar.
    Isto frequentemente resolve-se sozinho, portanto experimente recarregar a p\xE1gina.
    Caso contr\xE1rio, por favor contacte o administrador do site.
error-wasm-disabled-on-edge =
    O Ruffle falhou ao carregar o componente de ficheiro ".wasm" necess\xE1rio.
    Para corrigir isso, tente abrir as op\xE7\xF5es do seu navegador, clicando em "Privacidade, pesquisa e servi\xE7os", rolando para baixo e desativando "Melhore a sua seguran\xE7a na web".
    Isto permitir\xE1 ao seu navegador carregar os ficheiros ".wasm" necess\xE1rios.
    Se o problema persistir, talvez seja necess\xE1rio usar um navegador diferente.
error-javascript-conflict =
    O Ruffle encontrou um problema maior ao tentar inicializar.
    Parece que esta p\xE1gina usa c\xF3digo JavaScript que entra em conflito com o Ruffle.
    Se \xE9 o administrador do servidor, convidamo-lo a tentar carregar o ficheiro em numa p\xE1gina em branco.
error-javascript-conflict-outdated = Pode ainda tentar carregar uma vers\xE3o mais recente do Ruffle que talvez contorne o problema (a compila\xE7\xE3o atual est\xE1 desatualizada: { $buildDate }).
error-csp-conflict =
    O Ruffle encontrou um problema maior ao tentar inicializar.
    A Pol\xEDtica de Seguran\xE7a de Conte\xFAdo deste servidor n\xE3o permite que o componente ".wasm" necess\xE1rio seja executado.
    Se \xE9 o administrador do servidor, por favor consulte o wiki do Ruffle para obter ajuda.
error-unknown =
    O Ruffle encontrou um problema maior enquanto tentava mostrar este conte\xFAdo em Flash.
    { $outdated ->
        [true] Se \xE9 o administrador do servidor, por favor tente carregar uma vers\xE3o mais recente do Ruffle (a compila\xE7\xE3o atual est\xE1 desatualizada: { $buildDate }).
       *[false] N\xE3o era suposto isto ter acontecido, por isso agradecer\xEDamos muito se pudesse reportar a falha!
    }
`,"save-manager.ftl":`save-delete-prompt = Tem a certeza de que quer apagar esta grava\xE7\xE3o?
save-reload-prompt =
    A \xFAnica forma de { $action ->
        [delete] apagar
       *[replace] substituir
    } esta grava\xE7\xE3o sem um potencial conflito \xE9 recarregar este conte\xFAdo. Deseja continuar mesmo assim?
save-download = Descarregar
save-replace = Substituir
save-delete = Apagar
save-backup-all = Descarregar todas as grava\xE7\xF5es
`,"volume-controls.ftl":`volume-controls = Controlos de volume
volume-controls-mute = Silenciar
volume-controls-volume = Volume
`},"ro-RO":{"context_menu.ftl":`context-menu-download-swf = Descarc\u0103 .swf
context-menu-copy-debug-info = Copiaz\u0103 informa\u021Biile de depanare
context-menu-open-save-manager = Deschide managerul de salv\u0103ri
context-menu-about-ruffle =
    { $flavor ->
        [extension] Despre extensia Ruffle ({ $version })
       *[other] Despre Ruffle ({ $version })
    }
context-menu-hide = Ascunde acest meniu
context-menu-exit-fullscreen = Ie\u0219i din ecranul complet
context-menu-enter-fullscreen = Intr\u0103 \xEEn ecran complet
`,"messages.ftl":`message-cant-embed =
    Ruffle nu a putut rula Flash \xEEncorporat \xEEn aceast\u0103 pagin\u0103.
    Pute\u021Bi \xEEncerca s\u0103 deschide\u021Bi fi\u0219ierul \xEEntr-o fil\u0103 separat\u0103, pentru a evita aceast\u0103 problem\u0103.
panic-title = Ceva a mers prost :(
more-info = Mai multe informa\u021Bii
run-anyway = Ruleaz\u0103 oricum
continue = Continu\u0103
report-bug = Raporteaz\u0103 un bug
update-ruffle = Actualizeaz\u0103 Ruffle
ruffle-demo = Demo web
ruffle-desktop = Aplica\u021Bie desktop
ruffle-wiki = Vezi wikiul Ruffle
view-error-details = Vezi detaliile erorii
open-in-new-tab = Deschide \xEEntr-o fil\u0103 nou\u0103
click-to-unmute = D\u0103 click pentru a dezmu\u021Bi
error-file-protocol =
    Se pare c\u0103 rulezi Ruffle pe protocolul \u201Efile:\u201D.
    Acesta nu func\u021Bioneaz\u0103, deoarece browserele blocheaz\u0103 func\u021Bionarea multor func\u021Bii din motive de securitate.
    \xCEn schimb, te invit\u0103m s\u0103 configurezi un server local sau s\u0103 folose\u0219ti fie demoul web, fie aplica\u021Bia desktop.
error-javascript-config =
    Ruffle a \xEEnt\xE2mpinat o problem\u0103 major\u0103 din cauza unei configur\u0103ri incorecte a JavaScript.
    Dac\u0103 sunte\u021Bi administratorul serverului, v\u0103 invit\u0103m s\u0103 verifica\u021Bi detaliile de eroare pentru a afla care parametru este defect.
    Pute\u021Bi consulta \u0219i Ruffle wiki pentru ajutor.
error-wasm-not-found =
    Ruffle a e\u0219uat la \xEEnc\u0103rcarea componentei de fi\u0219ier ".wasm".
    Dac\u0103 sunte\u021Bi administratorul serverului, v\u0103 rug\u0103m s\u0103 v\u0103 asigura\u021Bi c\u0103 fi\u0219ierul a fost \xEEnc\u0103rcat corect.
    Dac\u0103 problema persist\u0103, poate fi necesar s\u0103 utiliza\u0163i setarea "publicPath": v\u0103 rug\u0103m s\u0103 consulta\u0163i Ruffle wiki pentru ajutor.
error-wasm-mime-type =
    Ruffle a \xEEnt\xE2mpinat o problem\u0103 major\u0103 \xEEn timp ce se \xEEncerca ini\u021Bializarea.
    Acest server web nu serve\u0219te ". asm" fi\u0219iere cu tipul corect MIME.
    Dac\u0103 sunte\u021Bi administrator de server, v\u0103 rug\u0103m s\u0103 consulta\u021Bi Ruffle wiki pentru ajutor.
error-swf-fetch =
    Ruffle a e\u0219uat la \xEEnc\u0103rcarea fi\u0219ierului Flash SWF.
    Motivul cel mai probabil este c\u0103 fi\u015Fierul nu mai exist\u0103, deci nu exist\u0103 nimic pentru Ruffle s\u0103 se \xEEncarce.
    \xCEncerca\u021Bi s\u0103 contacta\u021Bi administratorul site-ului web pentru ajutor.
error-swf-cors =
    Ruffle a e\u0219uat la \xEEnc\u0103rcarea fi\u0219ierului Flash SWF.
    Accesul la preluare a fost probabil blocat de politica CORS.
    Dac\u0103 sunte\u0163i administratorul serverului, v\u0103 rug\u0103m s\u0103 consulta\u0163i Ruffle wiki pentru ajutor.
error-wasm-cors =
    Ruffle a e\u0219uat \xEEn \xEEnc\u0103rcarea componentei de fi\u0219ier ".wasm".
    Accesul la preluare a fost probabil blocat de politica CORS.
    Dac\u0103 sunte\u0163i administratorul serverului, v\u0103 rug\u0103m s\u0103 consulta\u0163i Ruffle wiki pentru ajutor.
error-wasm-invalid =
    Ruffle a \xEEnt\xE2mpinat o problem\u0103 major\u0103 \xEEn timp ce se \xEEncearc\u0103 ini\u021Bializarea.
    Se pare c\u0103 aceast\u0103 pagin\u0103 are fi\u0219iere lips\u0103 sau invalide pentru rularea Ruffle.
    Dac\u0103 sunte\u0163i administratorul serverului, v\u0103 rug\u0103m s\u0103 consulta\u0163i Ruffle wiki pentru ajutor.
error-wasm-download =
    Ruffle a \xEEnt\xE2mpinat o problem\u0103 major\u0103 \xEEn timp ce \xEEncerca s\u0103 ini\u021Bializeze.
    Acest lucru se poate rezolva adesea, astfel \xEEnc\xE2t pute\u0163i \xEEncerca s\u0103 re\xEEnc\u0103rca\u0163i pagina.
    Altfel, v\u0103 rug\u0103m s\u0103 contacta\u0163i administratorul site-ului.
error-wasm-disabled-on-edge =
    Ruffle nu a putut \xEEnc\u0103rca componenta de fi\u0219ier ".wasm".
    Pentru a remedia acest lucru, \xEEncerca\u021Bi s\u0103 deschide\u021Bi set\u0103rile browser-ului dvs., ap\u0103s\xE2nd pe "Confiden\u021Bialitate, c\u0103utare \u0219i servicii", derul\xE2nd \xEEn jos \u0219i \xEEnchiz\xE2nd "\xCEmbun\u0103t\u0103\u021Be\u0219te-\u021Bi securitatea pe web".
    Acest lucru va permite browser-ului s\u0103 \xEEncarce fi\u0219ierele ".wasm" necesare.
    Dac\u0103 problema persist\u0103, ar putea fi necesar s\u0103 folosi\u021Bi un browser diferit.
error-javascript-conflict =
    Ruffle a \xEEnt\xE2mpinat o problem\u0103 major\u0103 \xEEn timp ce \xEEncerca s\u0103 ini\u021Bializeze.
    Se pare c\u0103 aceast\u0103 pagin\u0103 folose\u0219te codul JavaScript care intr\u0103 \xEEn conflict cu Ruffle.
    Dac\u0103 sunte\u0163i administratorul serverului, v\u0103 invit\u0103m s\u0103 \xEEnc\u0103rca\u0163i fi\u015Fierul pe o pagin\u0103 goal\u0103.
error-javascript-conflict-outdated = De asemenea, po\u021Bi \xEEncerca s\u0103 \xEEncarci o versiune mai recent\u0103 de Ruffle care poate ocoli problema (versiunea curent\u0103 este expirat\u0103: { $buildDate }).
error-csp-conflict =
    Ruffle a \xEEnt\xE2mpinat o problem\u0103 major\u0103 \xEEn timp ce se \xEEncerca ini\u021Bializarea.
    Politica de securitate a con\u021Binutului acestui server web nu permite serviciul necesar". asm" component\u0103 pentru a rula.
    Dac\u0103 sunte\u021Bi administratorul de server, consulta\u021Bi Ruffle wiki pentru ajutor.
error-unknown =
    Ruffle a \xEEnt\xE2mpinat o problem\u0103 major\u0103 \xEEn timp ce \xEEncerca s\u0103 afi\u0219eze acest con\u021Binut Flash.
    { $outdated ->
        [true] Dac\u0103 e\u0219ti administratorul serverului, te rug\u0103m s\u0103 \xEEncerci s\u0103 \xEEncarci o versiune mai recent\u0103 de Ruffle (versiunea actual\u0103 este dep\u0103\u015Fit\u0103: { $buildDate }).
       *[false] Acest lucru nu ar trebui s\u0103 se \xEEnt\xE2mple, a\u0219a c\u0103 am aprecia foarte mult dac\u0103 ai putea trimite un bug!
    }
`,"save-manager.ftl":`save-delete-prompt = Sigur vrei s\u0103 \u0219tergi acest fi\u0219ier de salvare?
save-reload-prompt =
    Singura cale de a { $action ->
        [delete] \u0219terge
       *[replace] \xEEnlocui
    } acest fi\u0219ier de salvare f\u0103r\u0103 un conflict poten\u021Bial este de a re\xEEnc\u0103rca acest con\u021Binut. Dore\u0219ti s\u0103 continui oricum?
save-download = Descarc\u0103
save-replace = \xCEnlocuie\u0219te
save-delete = \u0218terge
`,"volume-controls.ftl":`volume-controls = Comenzi pentru volum
`},"ru-RU":{"context_menu.ftl":`context-menu-download-swf = \u0421\u043A\u0430\u0447\u0430\u0442\u044C .swf
context-menu-copy-debug-info = \u041A\u043E\u043F\u0438\u0440\u043E\u0432\u0430\u0442\u044C \u043E\u0442\u043B\u0430\u0434\u043E\u0447\u043D\u0443\u044E \u0438\u043D\u0444\u043E\u0440\u043C\u0430\u0446\u0438\u044E
context-menu-open-save-manager = \u041C\u0435\u043D\u0435\u0434\u0436\u0435\u0440 \u0441\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u0438\u0439
context-menu-about-ruffle =
    { $flavor ->
        [extension] \u041E \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043D\u0438\u0438 Ruffle ({ $version })
       *[other] \u041E Ruffle ({ $version })
    }
context-menu-hide = \u0421\u043A\u0440\u044B\u0442\u044C \u044D\u0442\u043E \u043C\u0435\u043D\u044E
context-menu-exit-fullscreen = \u041E\u043A\u043E\u043D\u043D\u044B\u0439 \u0440\u0435\u0436\u0438\u043C
context-menu-enter-fullscreen = \u041F\u043E\u043B\u043D\u043E\u044D\u043A\u0440\u0430\u043D\u043D\u044B\u0439 \u0440\u0435\u0436\u0438\u043C
context-menu-volume-controls = \u0413\u0440\u043E\u043C\u043A\u043E\u0441\u0442\u044C
`,"messages.ftl":`message-cant-embed =
    Ruffle \u043D\u0435 \u0441\u043C\u043E\u0433 \u0437\u0430\u043F\u0443\u0441\u0442\u0438\u0442\u044C Flash, \u0438\u0441\u043F\u043E\u043B\u044C\u0437\u0443\u0435\u043C\u044B\u0439 \u043D\u0430 \u044D\u0442\u043E\u0439 \u0441\u0442\u0440\u0430\u043D\u0438\u0446\u0435.
    \u0427\u0442\u043E\u0431\u044B \u043E\u0431\u043E\u0439\u0442\u0438 \u044D\u0442\u0443 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u0443, \u0432\u044B \u043C\u043E\u0436\u0435\u0442\u0435 \u043F\u043E\u043F\u0440\u043E\u0431\u043E\u0432\u0430\u0442\u044C \u043E\u0442\u043A\u0440\u044B\u0442\u044C \u0444\u0430\u0439\u043B \u0432 \u043E\u0442\u0434\u0435\u043B\u044C\u043D\u043E\u0439 \u0432\u043A\u043B\u0430\u0434\u043A\u0435.
panic-title = \u0427\u0442\u043E-\u0442\u043E \u043F\u043E\u0448\u043B\u043E \u043D\u0435 \u0442\u0430\u043A :(
more-info = \u041F\u043E\u0434\u0440\u043E\u0431\u043D\u0435\u0435
run-anyway = \u0412\u0441\u0451 \u0440\u0430\u0432\u043D\u043E \u0437\u0430\u043F\u0443\u0441\u0442\u0438\u0442\u044C
continue = \u041F\u0440\u043E\u0434\u043E\u043B\u0436\u0438\u0442\u044C
report-bug = \u0421\u043E\u043E\u0431\u0449\u0438\u0442\u044C \u043E\u0431 \u043E\u0448\u0438\u0431\u043A\u0435
update-ruffle = \u041E\u0431\u043D\u043E\u0432\u0438\u0442\u044C Ruffle
ruffle-demo = \u0412\u0435\u0431-\u0434\u0435\u043C\u043E
ruffle-desktop = \u041D\u0430\u0441\u0442\u043E\u043B\u044C\u043D\u043E\u0435 \u043F\u0440\u0438\u043B\u043E\u0436\u0435\u043D\u0438\u0435
ruffle-wiki = \u041E\u0442\u043A\u0440\u044B\u0442\u044C \u0432\u0438\u043A\u0438 Ruffle
enable-hardware-acceleration = \u041F\u043E\u0445\u043E\u0436\u0435, \u0447\u0442\u043E \u0430\u043F\u043F\u0430\u0440\u0430\u0442\u043D\u043E\u0435 \u0443\u0441\u043A\u043E\u0440\u0435\u043D\u0438\u0435 \u043D\u0435 \u0432\u043A\u043B\u044E\u0447\u0435\u043D\u043E. \u0425\u043E\u0442\u044C Ruffle \u0438 \u0431\u0443\u0434\u0435\u0442 \u0440\u0430\u0431\u043E\u0442\u0430\u0442\u044C, \u043E\u043D \u043C\u043E\u0436\u0435\u0442 \u0431\u044B\u0442\u044C \u043D\u0435\u043E\u043F\u0440\u0430\u0432\u0434\u0430\u043D\u043D\u043E \u043C\u0435\u0434\u043B\u0435\u043D\u043D\u044B\u043C. \u041E \u0442\u043E\u043C, \u043A\u0430\u043A \u0432\u043A\u043B\u044E\u0447\u0438\u0442\u044C \u0430\u043F\u043F\u0430\u0440\u0430\u0442\u043D\u043E\u0435 \u0443\u0441\u043A\u043E\u0440\u0435\u043D\u0438\u0435, \u043C\u043E\u0436\u043D\u043E \u0443\u0437\u043D\u0430\u0442\u044C, \u043F\u0435\u0440\u0435\u0439\u0434\u044F \u043F\u043E \u0441\u0441\u044B\u043B\u043A\u0435.
view-error-details = \u0421\u0432\u0435\u0434\u0435\u043D\u0438\u044F \u043E\u0431 \u043E\u0448\u0438\u0431\u043A\u0435
open-in-new-tab = \u041E\u0442\u043A\u0440\u044B\u0442\u044C \u0432 \u043D\u043E\u0432\u043E\u0439 \u0432\u043A\u043B\u0430\u0434\u043A\u0435
click-to-unmute = \u0412\u043A\u043B\u044E\u0447\u0438\u0442\u044C \u0437\u0432\u0443\u043A
error-file-protocol =
    \u041F\u043E\u0445\u043E\u0436\u0435, \u0447\u0442\u043E \u0432\u044B \u0437\u0430\u043F\u0443\u0441\u043A\u0430\u0435\u0442\u0435 Ruffle \u043F\u043E \u043F\u0440\u043E\u0442\u043E\u043A\u043E\u043B\u0443 "file:".
    \u042D\u0442\u043E \u043D\u0435 \u0440\u0430\u0431\u043E\u0442\u0430\u0435\u0442, \u043F\u043E\u0441\u043A\u043E\u043B\u044C\u043A\u0443 \u0431\u0440\u0430\u0443\u0437\u0435\u0440\u044B \u0431\u043B\u043E\u043A\u0438\u0440\u0443\u044E\u0442 \u0440\u0430\u0431\u043E\u0442\u0443 \u043C\u043D\u043E\u0433\u0438\u0445 \u0444\u0443\u043D\u043A\u0446\u0438\u0439 \u043F\u043E \u0441\u043E\u043E\u0431\u0440\u0430\u0436\u0435\u043D\u0438\u044F\u043C \u0431\u0435\u0437\u043E\u043F\u0430\u0441\u043D\u043E\u0441\u0442\u0438.
    \u0412\u043C\u0435\u0441\u0442\u043E \u044D\u0442\u043E\u0433\u043E \u043C\u044B \u043F\u0440\u0435\u0434\u043B\u0430\u0433\u0430\u0435\u043C \u0432\u0430\u043C \u0438\u0441\u043F\u043E\u043B\u044C\u0437\u043E\u0432\u0430\u0442\u044C \u043D\u0430\u0441\u0442\u043E\u043B\u044C\u043D\u043E\u0435 \u043F\u0440\u0438\u043B\u043E\u0436\u0435\u043D\u0438\u0435, \u0432\u0435\u0431-\u0434\u0435\u043C\u043E \u0438\u043B\u0438 \u043D\u0430\u0441\u0442\u0440\u043E\u0438\u0442\u044C \u043B\u043E\u043A\u0430\u043B\u044C\u043D\u044B\u0439 \u0441\u0435\u0440\u0432\u0435\u0440.
error-javascript-config =
    \u0412\u043E\u0437\u043D\u0438\u043A\u043B\u0430 \u0441\u0435\u0440\u044C\u0451\u0437\u043D\u0430\u044F \u043E\u0448\u0438\u0431\u043A\u0430 \u0438\u0437-\u0437\u0430 \u043D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u043E\u0439 \u043A\u043E\u043D\u0444\u0438\u0433\u0443\u0440\u0430\u0446\u0438\u0438 JavaScript.
    \u0415\u0441\u043B\u0438 \u0432\u044B \u044F\u0432\u043B\u044F\u0435\u0442\u0435\u0441\u044C \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u043E\u043C \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043C\u044B \u043F\u0440\u0435\u0434\u043B\u0430\u0433\u0430\u0435\u043C \u0432\u0430\u043C \u043F\u0440\u043E\u0432\u0435\u0440\u0438\u0442\u044C \u0434\u0435\u0442\u0430\u043B\u0438 \u043E\u0448\u0438\u0431\u043A\u0438, \u0447\u0442\u043E\u0431\u044B \u0432\u044B\u044F\u0441\u043D\u0438\u0442\u044C, \u043A\u0430\u043A\u043E\u0439 \u043F\u0430\u0440\u0430\u043C\u0435\u0442\u0440 \u0434\u0430\u043B \u0441\u0431\u043E\u0439.
    \u0412\u044B \u0442\u0430\u043A\u0436\u0435 \u043C\u043E\u0436\u0435\u0442\u0435 \u043E\u0431\u0440\u0430\u0442\u0438\u0442\u044C\u0441\u044F \u0437\u0430 \u043F\u043E\u043C\u043E\u0449\u044C\u044E \u043A \u0432\u0438\u043A\u0438 Ruffle.
error-wasm-not-found =
    Ruffle \u043D\u0435 \u0443\u0434\u0430\u043B\u043E\u0441\u044C \u0437\u0430\u043F\u0443\u0441\u0442\u0438\u0442\u044C \u043D\u0435\u043E\u0431\u0445\u043E\u0434\u0438\u043C\u044B\u0439 \u043A\u043E\u043C\u043F\u043E\u043D\u0435\u043D\u0442 \u0444\u0430\u0439\u043B\u0430 ".wasm".
    \u0415\u0441\u043B\u0438 \u0432\u044B \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440 \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043F\u043E\u0436\u0430\u043B\u0443\u0439\u0441\u0442\u0430, \u0443\u0431\u0435\u0434\u0438\u0442\u0435\u0441\u044C, \u0447\u0442\u043E \u0444\u0430\u0439\u043B \u0431\u044B\u043B \u0437\u0430\u0433\u0440\u0443\u0436\u0435\u043D \u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u043E.
    \u0415\u0441\u043B\u0438 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u0430 \u043D\u0435 \u0443\u0441\u0442\u0440\u0430\u043D\u044F\u0435\u0442\u0441\u044F, \u0432\u0430\u043C \u043C\u043E\u0436\u0435\u0442 \u043F\u043E\u0442\u0440\u0435\u0431\u043E\u0432\u0430\u0442\u044C\u0441\u044F \u0438\u0441\u043F\u043E\u043B\u044C\u0437\u043E\u0432\u0430\u0442\u044C \u043D\u0430\u0441\u0442\u0440\u043E\u0439\u043A\u0443 "publicPath": \u043E\u0431\u0440\u0430\u0442\u0438\u0442\u0435\u0441\u044C \u043A \u0432\u0438\u043A\u0438 Ruffle.
error-wasm-mime-type =
    Ruffle \u0441\u0442\u043E\u043B\u043A\u043D\u0443\u043B\u0441\u044F \u0441 \u0441\u0435\u0440\u044C\u0451\u0437\u043D\u043E\u0439 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u043E\u0439 \u0432\u043E \u0432\u0440\u0435\u043C\u044F \u0438\u043D\u0438\u0446\u0438\u0430\u043B\u0438\u0437\u0430\u0446\u0438\u0438.
    \u042D\u0442\u043E\u0442 \u0432\u0435\u0431-\u0441\u0435\u0440\u0432\u0435\u0440 \u043D\u0435 \u043F\u0440\u0435\u0434\u043E\u0441\u0442\u0430\u0432\u043B\u044F\u0435\u0442 \u0444\u0430\u0439\u043B\u044B ".wasm" \u0441 \u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u044B\u043C \u0442\u0438\u043F\u043E\u043C MIME.
    \u0415\u0441\u043B\u0438 \u0432\u044B \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440 \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043E\u0431\u0440\u0430\u0442\u0438\u0442\u0435\u0441\u044C \u0437\u0430 \u043F\u043E\u043C\u043E\u0449\u044C\u044E \u043A \u0432\u0438\u043A\u0438 Ruffle.
error-swf-fetch =
    Ruffle \u043D\u0435 \u0443\u0434\u0430\u043B\u043E\u0441\u044C \u0437\u0430\u043F\u0443\u0441\u0442\u0438\u0442\u044C SWF-\u0444\u0430\u0439\u043B Flash.
    \u0412\u0435\u0440\u043E\u044F\u0442\u043D\u0435\u0435 \u0432\u0441\u0435\u0433\u043E, \u0444\u0430\u0439\u043B \u0431\u043E\u043B\u044C\u0448\u0435 \u043D\u0435 \u0441\u0443\u0449\u0435\u0441\u0442\u0432\u0443\u0435\u0442, \u043F\u043E\u044D\u0442\u043E\u043C\u0443 Ruffle \u043D\u0435\u0447\u0435\u0433\u043E \u0437\u0430\u0433\u0440\u0443\u0436\u0430\u0442\u044C.
    \u041F\u043E\u043F\u0440\u043E\u0431\u0443\u0439\u0442\u0435 \u0441\u0432\u044F\u0437\u0430\u0442\u044C\u0441\u044F \u0441 \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u043E\u043C \u0441\u0430\u0439\u0442\u0430 \u0434\u043B\u044F \u043F\u043E\u043B\u0443\u0447\u0435\u043D\u0438\u044F \u043F\u043E\u043C\u043E\u0449\u0438.
error-swf-cors =
    Ruffle \u043D\u0435 \u0443\u0434\u0430\u043B\u043E\u0441\u044C \u0437\u0430\u043F\u0443\u0441\u0442\u0438\u0442\u044C SWF-\u0444\u0430\u0439\u043B Flash.
    \u0421\u043A\u043E\u0440\u0435\u0435 \u0432\u0441\u0435\u0433\u043E, \u0434\u043E\u0441\u0442\u0443\u043F \u043A \u0444\u0430\u0439\u043B\u0443 \u0431\u044B\u043B \u0437\u0430\u0431\u043B\u043E\u043A\u0438\u0440\u043E\u0432\u0430\u043D \u043F\u043E\u043B\u0438\u0442\u0438\u043A\u043E\u0439 CORS.
    \u0415\u0441\u043B\u0438 \u0432\u044B \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440 \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043E\u0431\u0440\u0430\u0442\u0438\u0442\u0435\u0441\u044C \u0437\u0430 \u043F\u043E\u043C\u043E\u0449\u044C\u044E \u043A \u0432\u0438\u043A\u0438 Ruffle.
error-wasm-cors =
    Ruffle \u043D\u0435 \u0443\u0434\u0430\u043B\u043E\u0441\u044C \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044C \u043D\u0435\u043E\u0431\u0445\u043E\u0434\u0438\u043C\u044B\u0439 \u043A\u043E\u043C\u043F\u043E\u043D\u0435\u043D\u0442 \u0444\u0430\u0439\u043B\u0430 ".wasm".
    \u0421\u043A\u043E\u0440\u0435\u0435 \u0432\u0441\u0435\u0433\u043E, \u0434\u043E\u0441\u0442\u0443\u043F \u043A \u0444\u0430\u0439\u043B\u0443 \u0431\u044B\u043B \u0437\u0430\u0431\u043B\u043E\u043A\u0438\u0440\u043E\u0432\u0430\u043D \u043F\u043E\u043B\u0438\u0442\u0438\u043A\u043E\u0439 CORS.
    \u0415\u0441\u043B\u0438 \u0432\u044B \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440 \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043E\u0431\u0440\u0430\u0442\u0438\u0442\u0435\u0441\u044C \u0437\u0430 \u043F\u043E\u043C\u043E\u0449\u044C\u044E \u043A \u0432\u0438\u043A\u0438 Ruffle.
error-wasm-invalid =
    Ruffle \u0441\u0442\u043E\u043B\u043A\u043D\u0443\u043B\u0441\u044F \u0441 \u0441\u0435\u0440\u044C\u0451\u0437\u043D\u043E\u0439 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u043E\u0439 \u0432\u043E \u0432\u0440\u0435\u043C\u044F \u0438\u043D\u0438\u0446\u0438\u0430\u043B\u0438\u0437\u0430\u0446\u0438\u0438.
    \u041F\u043E\u0445\u043E\u0436\u0435, \u0447\u0442\u043E \u043D\u0430 \u044D\u0442\u043E\u0439 \u0441\u0442\u0440\u0430\u043D\u0438\u0446\u0435 \u043E\u0442\u0441\u0443\u0442\u0441\u0442\u0432\u0443\u044E\u0442 \u0444\u0430\u0439\u043B\u044B \u0434\u043B\u044F \u0437\u0430\u043F\u0443\u0441\u043A\u0430 Ruffle \u0438\u043B\u0438 \u043E\u043D\u0438 \u043D\u0435\u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0442\u0435\u043B\u044C\u043D\u044B.
    \u0415\u0441\u043B\u0438 \u0432\u044B \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440 \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043E\u0431\u0440\u0430\u0442\u0438\u0442\u0435\u0441\u044C \u0437\u0430 \u043F\u043E\u043C\u043E\u0449\u044C\u044E \u043A \u0432\u0438\u043A\u0438 Ruffle.
error-wasm-download =
    Ruffle \u0441\u0442\u043E\u043B\u043A\u043D\u0443\u043B\u0441\u044F \u0441 \u0441\u0435\u0440\u044C\u0451\u0437\u043D\u043E\u0439 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u043E\u0439 \u0432\u043E \u0432\u0440\u0435\u043C\u044F \u0438\u043D\u0438\u0446\u0438\u0430\u043B\u0438\u0437\u0430\u0446\u0438\u0438.
    \u0427\u0430\u0449\u0435 \u0432\u0441\u0435\u0433\u043E \u044D\u0442\u0430 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u0430 \u0443\u0441\u0442\u0440\u0430\u043D\u044F\u0435\u0442\u0441\u044F \u0441\u0430\u043C\u0430 \u0441\u043E\u0431\u043E\u044E, \u043F\u043E\u044D\u0442\u043E\u043C\u0443 \u0432\u044B \u043C\u043E\u0436\u0435\u0442\u0435 \u043F\u0440\u043E\u0441\u0442\u043E \u043F\u0435\u0440\u0435\u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044C \u0441\u0442\u0440\u0430\u043D\u0438\u0446\u0443.
    \u0415\u0441\u043B\u0438 \u043E\u0448\u0438\u0431\u043A\u0430 \u043F\u0440\u043E\u0434\u043E\u043B\u0436\u0430\u0435\u0442 \u043F\u043E\u044F\u0432\u043B\u044F\u0442\u044C\u0441\u044F, \u0441\u0432\u044F\u0436\u0438\u0442\u0435\u0441\u044C \u0441 \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u043E\u043C \u0441\u0430\u0439\u0442\u0430.
error-wasm-disabled-on-edge =
    Ruffle \u043D\u0435 \u0443\u0434\u0430\u043B\u043E\u0441\u044C \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044C \u043D\u0435\u043E\u0431\u0445\u043E\u0434\u0438\u043C\u044B\u0439 \u043A\u043E\u043C\u043F\u043E\u043D\u0435\u043D\u0442 \u0444\u0430\u0439\u043B\u0430 ".wasm".
    \u0427\u0442\u043E\u0431\u044B \u0438\u0441\u043F\u0440\u0430\u0432\u0438\u0442\u044C \u044D\u0442\u0443 \u043E\u0448\u0438\u0431\u043A\u0443, \u043F\u043E\u043F\u0440\u043E\u0431\u0443\u0439\u0442\u0435 \u043E\u0442\u043A\u043B\u044E\u0447\u0438\u0442\u044C \u0432 \u043D\u0430\u0441\u0442\u0440\u043E\u0439\u043A\u0430\u0445 \u0431\u0440\u0430\u0443\u0437\u0435\u0440\u0430 \u0434\u043E\u043F\u043E\u043B\u043D\u0438\u0442\u0435\u043B\u044C\u043D\u0443\u044E \u043A\u043E\u043D\u0444\u0438\u0434\u0435\u043D\u0446\u0438\u0430\u043B\u044C\u043D\u043E\u0441\u0442\u044C. \u042D\u0442\u043E \u043F\u043E\u0437\u0432\u043E\u043B\u0438\u0442 \u0431\u0440\u0430\u0443\u0437\u0435\u0440\u0443 \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044C \u043D\u0435\u043E\u0431\u0445\u043E\u0434\u0438\u043C\u044B\u0435 WASM-\u0444\u0430\u0439\u043B\u044B.
    \u0415\u0441\u043B\u0438 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u0430 \u043E\u0441\u0442\u0430\u043B\u0430\u0441\u044C, \u0432\u0430\u043C \u043C\u043E\u0436\u0435\u0442 \u043F\u043E\u0442\u0440\u0435\u0431\u043E\u0432\u0430\u0442\u044C\u0441\u044F \u0434\u0440\u0443\u0433\u043E\u0439 \u0431\u0440\u0430\u0443\u0437\u0435\u0440.
error-javascript-conflict =
    Ruffle \u0441\u0442\u043E\u043B\u043A\u043D\u0443\u043B\u0441\u044F \u0441 \u0441\u0435\u0440\u044C\u0451\u0437\u043D\u043E\u0439 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u043E\u0439 \u0432\u043E \u0432\u0440\u0435\u043C\u044F \u0438\u043D\u0438\u0446\u0438\u0430\u043B\u0438\u0437\u0430\u0446\u0438\u0438.
    \u041F\u043E\u0445\u043E\u0436\u0435, \u0447\u0442\u043E \u044D\u0442\u0430 \u0441\u0442\u0440\u0430\u043D\u0438\u0446\u0430 \u0438\u0441\u043F\u043E\u043B\u044C\u0437\u0443\u0435\u0442 \u043A\u043E\u043D\u0444\u043B\u0438\u043A\u0442\u0443\u044E\u0449\u0438\u0439 \u0441 Ruffle \u043A\u043E\u0434 JavaScript.
    \u0415\u0441\u043B\u0438 \u0432\u044B \u044F\u0432\u043B\u044F\u0435\u0442\u0435\u0441\u044C \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u043E\u043C \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043C\u044B \u043F\u0440\u0435\u0434\u043B\u0430\u0433\u0430\u0435\u043C \u0432\u0430\u043C \u043F\u043E\u043F\u0440\u043E\u0431\u043E\u0432\u0430\u0442\u044C \u0437\u0430\u043F\u0443\u0441\u0442\u0438\u0442\u044C \u0444\u0430\u0439\u043B \u043D\u0430 \u043F\u0443\u0441\u0442\u043E\u0439 \u0441\u0442\u0440\u0430\u043D\u0438\u0446\u0435.
error-javascript-conflict-outdated = \u0412\u044B \u0442\u0430\u043A\u0436\u0435 \u043C\u043E\u0436\u0435\u0442\u0435 \u043F\u043E\u043F\u0440\u043E\u0431\u043E\u0432\u0430\u0442\u044C \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044C \u043F\u043E\u0441\u043B\u0435\u0434\u043D\u044E\u044E \u0432\u0435\u0440\u0441\u0438\u044E Ruffle, \u043A\u043E\u0442\u043E\u0440\u0430\u044F \u043C\u043E\u0436\u0435\u0442 \u043E\u0431\u043E\u0439\u0442\u0438 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u0443 (\u0442\u0435\u043A\u0443\u0449\u0430\u044F \u0432\u0435\u0440\u0441\u0438\u044F \u0443\u0441\u0442\u0430\u0440\u0435\u043B\u0430: { $buildDate }).
error-csp-conflict =
    Ruffle \u0441\u0442\u043E\u043B\u043A\u043D\u0443\u043B\u0441\u044F \u0441 \u0441\u0435\u0440\u044C\u0451\u0437\u043D\u043E\u0439 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u043E\u0439 \u0432\u043E \u0432\u0440\u0435\u043C\u044F \u0438\u043D\u0438\u0446\u0438\u0430\u043B\u0438\u0437\u0430\u0446\u0438\u0438.
    \u041F\u043E\u043B\u0438\u0442\u0438\u043A\u0430 \u0431\u0435\u0437\u043E\u043F\u0430\u0441\u043D\u043E\u0441\u0442\u0438 \u0441\u043E\u0434\u0435\u0440\u0436\u0438\u043C\u043E\u0433\u043E \u044D\u0442\u043E\u0433\u043E \u0432\u0435\u0431-\u0441\u0435\u0440\u0432\u0435\u0440\u0430 \u043D\u0435 \u043F\u043E\u0437\u0432\u043E\u043B\u044F\u0435\u0442 \u0438\u0441\u043F\u043E\u043B\u044C\u0437\u043E\u0432\u0430\u0442\u044C \u0442\u0440\u0435\u0431\u0443\u0435\u043C\u044B\u0435 \u043A\u043E\u043C\u043F\u043E\u043D\u0435\u043D\u0442\u044B \u0434\u043B\u044F \u0437\u0430\u043F\u0443\u0441\u043A\u0430 ".wasm".
    \u0415\u0441\u043B\u0438 \u0432\u044B \u044F\u0432\u043B\u044F\u0435\u0442\u0435\u0441\u044C \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u043E\u043C \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043E\u0431\u0440\u0430\u0442\u0438\u0442\u0435\u0441\u044C \u0437\u0430 \u043F\u043E\u043C\u043E\u0449\u044C\u044E \u043A \u0432\u0438\u043A\u0438 Ruffle.
error-unknown =
    Ruffle \u0441\u0442\u043E\u043B\u043A\u043D\u0443\u043B\u0441\u044F \u0441 \u0441\u0435\u0440\u044C\u0451\u0437\u043D\u043E\u0439 \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u043E\u0439 \u043F\u0440\u0438 \u043F\u043E\u043F\u044B\u0442\u043A\u0435 \u043E\u0442\u043E\u0431\u0440\u0430\u0437\u0438\u0442\u044C \u044D\u0442\u043E\u0442 Flash-\u043A\u043E\u043D\u0442\u0435\u043D\u0442.
    { $outdated ->
        [true] \u0415\u0441\u043B\u0438 \u0432\u044B \u0430\u0434\u043C\u0438\u043D\u0438\u0441\u0442\u0440\u0430\u0442\u043E\u0440 \u0441\u0435\u0440\u0432\u0435\u0440\u0430, \u043F\u043E\u043F\u0440\u043E\u0431\u0443\u0439\u0442\u0435 \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044C \u0431\u043E\u043B\u0435\u0435 \u043D\u043E\u0432\u0443\u044E \u0432\u0435\u0440\u0441\u0438\u044E Ruffle (\u0442\u0435\u043A\u0443\u0449\u0430\u044F \u0432\u0435\u0440\u0441\u0438\u044F \u0443\u0441\u0442\u0430\u0440\u0435\u043B\u0430: { $buildDate }).
       *[false] \u042D\u0442\u043E\u0433\u043E \u043D\u0435 \u0434\u043E\u043B\u0436\u043D\u043E \u043F\u0440\u043E\u0438\u0441\u0445\u043E\u0434\u0438\u0442\u044C, \u043F\u043E\u044D\u0442\u043E\u043C\u0443 \u043C\u044B \u0431\u0443\u0434\u0435\u043C \u043E\u0447\u0435\u043D\u044C \u043F\u0440\u0438\u0437\u043D\u0430\u0442\u0435\u043B\u044C\u043D\u044B, \u0435\u0441\u043B\u0438 \u0432\u044B \u0441\u043E\u043E\u0431\u0449\u0438\u0442\u0435 \u043D\u0430\u043C \u043E\u0431 \u043E\u0448\u0438\u0431\u043A\u0435!
    }
`,"save-manager.ftl":`save-delete-prompt = \u0423\u0434\u0430\u043B\u0438\u0442\u044C \u044D\u0442\u043E\u0442 \u0444\u0430\u0439\u043B \u0441\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u0438\u044F?
save-reload-prompt =
    \u0415\u0434\u0438\u043D\u0441\u0442\u0432\u0435\u043D\u043D\u044B\u0439 \u0441\u043F\u043E\u0441\u043E\u0431 { $action ->
        [delete] \u0443\u0434\u0430\u043B\u0438\u0442\u044C
       *[replace] \u0437\u0430\u043C\u0435\u043D\u0438\u0442\u044C
    } \u044D\u0442\u043E\u0442 \u0444\u0430\u0439\u043B \u0441\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u0438\u044F \u0431\u0435\u0437 \u043F\u043E\u0442\u0435\u043D\u0446\u0438\u0430\u043B\u044C\u043D\u043E\u0433\u043E \u043A\u043E\u043D\u0444\u043B\u0438\u043A\u0442\u0430 \u2013 \u043F\u0435\u0440\u0435\u0437\u0430\u043F\u0443\u0441\u0442\u0438\u0442\u044C \u0437\u0430\u043F\u0443\u0449\u0435\u043D\u043D\u044B\u0439 \u043A\u043E\u043D\u0442\u0435\u043D\u0442. \u0412\u0441\u0451 \u0440\u0430\u0432\u043D\u043E \u043F\u0440\u043E\u0434\u043E\u043B\u0436\u0438\u0442\u044C?
save-download = \u0421\u043A\u0430\u0447\u0430\u0442\u044C
save-replace = \u0417\u0430\u043C\u0435\u043D\u0438\u0442\u044C
save-delete = \u0423\u0434\u0430\u043B\u0438\u0442\u044C
save-backup-all = \u0421\u043A\u0430\u0447\u0430\u0442\u044C \u0432\u0441\u0435 \u0441\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u0438\u044F
`,"volume-controls.ftl":`volume-controls = \u0420\u0435\u0433\u0443\u043B\u0438\u0440\u043E\u0432\u043A\u0430 \u0433\u0440\u043E\u043C\u043A\u043E\u0441\u0442\u0438
volume-controls-mute = \u0411\u0435\u0437 \u0437\u0432\u0443\u043A\u0430
volume-controls-volume = \u0413\u0440\u043E\u043C\u043A\u043E\u0441\u0442\u044C
`},"sk-SK":{"context_menu.ftl":`context-menu-download-swf = Stiahnu\u0165 .swf
context-menu-copy-debug-info = Skop\xEDrova\u0165 debug info
context-menu-open-save-manager = Otvori\u0165 spr\xE1vcu ulo\u017Een\xED
context-menu-about-ruffle =
    { $flavor ->
        [extension] O Ruffle roz\u0161\xEDren\xED ({ $version })
       *[other] O Ruffle ({ $version })
    }
context-menu-hide = Skry\u0165 menu
context-menu-exit-fullscreen = Ukon\u010Di\u0165 re\u017Eim celej obrazovky
context-menu-enter-fullscreen = Prejs\u0165 do re\u017Eimu celej obrazovky
context-menu-volume-controls = Ovl\xE1danie hlasitosti
`,"messages.ftl":`message-cant-embed =
    Ruffle nemohol spusti\u0165 Flash vlo\u017Een\xFD na tejto str\xE1nke.
    M\xF4\u017Eete sa pok\xFAsi\u0165 otvori\u0165 s\xFAbor na samostatnej karte, aby ste sa vyhli tomuto probl\xE9mu.
panic-title = Nie\u010Do sa pokazilo :(
more-info = Viac inform\xE1ci\xED
run-anyway = Spusti\u0165 aj tak
continue = Pokra\u010Dova\u0165
report-bug = Nahl\xE1si\u0165 chybu
update-ruffle = Aktualizova\u0165 Ruffle
ruffle-demo = Web Demo
ruffle-desktop = Desktopov\xE1 aplik\xE1cia
ruffle-wiki = Zobrazi\u0165 Ruffle Wiki
enable-hardware-acceleration = Zd\xE1 sa, \u017Ee hardv\xE9rov\xE1 akceler\xE1cia nie je povolen\xE1. Aj ke\u010F Ruffle funguje spr\xE1vne, m\xF4\u017Ee by\u0165 neprimerane pomal\xFD. Ako povoli\u0165 hardv\xE9rov\xFA akceler\xE1ciu zist\xEDte na tomto odkaze.
view-error-details = Zobrazi\u0165 podrobnosti o chybe
open-in-new-tab = Otvori\u0165 na novej karte
click-to-unmute = Kliknut\xEDm zapnete zvuk
error-file-protocol =
    Zd\xE1 sa, \u017Ee pou\u017E\xEDvate Ruffle na protokole "file:".
    To nie je mo\u017En\xE9, preto\u017Ee prehliada\u010De blokuj\xFA fungovanie mnoh\xFDch funkci\xED z bezpe\u010Dnostn\xFDch d\xF4vodov.
    Namiesto toho v\xE1m odpor\xFA\u010Dame nastavi\u0165 lok\xE1lny server alebo pou\u017Ei\u0165 web demo \u010Di desktopov\xFA aplik\xE1ciu.
error-javascript-config =
    Ruffle narazil na probl\xE9m v d\xF4sledku nespr\xE1vnej konfigur\xE1cie JavaScriptu.
    Ak ste spr\xE1vcom servera, odpor\xFA\u010Dame v\xE1m skontrolova\u0165 podrobnosti o chybe, aby ste zistili, ktor\xFD parameter je chybn\xFD.
    Pomoc m\xF4\u017Eete z\xEDska\u0165 aj na wiki Ruffle.
error-wasm-not-found =
    Ruffle sa nepodarilo na\u010D\xEDta\u0165 po\u017Eadovan\xFD komponent s\xFAboru \u201E.wasm\u201C.
    Ak ste spr\xE1vcom servera, skontrolujte, \u010Di bol s\xFAbor spr\xE1vne nahran\xFD.
    Ak probl\xE9m pretrv\xE1va, mo\u017Eno budete musie\u0165 pou\u017Ei\u0165 nastavenie \u201EpublicPath\u201C: pomoc n\xE1jdete na wiki Ruffle.
error-wasm-mime-type =
    Ruffle narazil na probl\xE9m pri pokuse o inicializ\xE1ciu.
    Tento webov\xFD server neposkytuje s\xFAbory \u201E.wasm\u201C so spr\xE1vnym typom MIME.
    Ak ste spr\xE1vcom servera, pomoc n\xE1jdete na Ruffle wiki.
error-swf-fetch =
    Ruffle sa nepodarilo na\u010D\xEDta\u0165 SWF s\xFAbor Flash.
    Najpravdepodobnej\u0161\xEDm d\xF4vodom je, \u017Ee s\xFAbor u\u017E neexistuje, tak\u017Ee Ruffle nem\xE1 \u010Do na\u010D\xEDta\u0165.
    Sk\xFAste po\u017Eiada\u0165 o pomoc spr\xE1vcu webovej lokality.
error-swf-cors =
    Ruffle sa nepodarilo na\u010D\xEDta\u0165 SWF s\xFAbor Flash.
    Pr\xEDstup k na\u010D\xEDtaniu bol pravdepodobne zablokovan\xFD politikou CORS.
    Ak ste spr\xE1vcom servera, pomoc n\xE1jdete na Ruffle wiki.
error-wasm-cors =
    Ruffle sa nepodarilo na\u010D\xEDta\u0165 po\u017Eadovan\xFD komponent s\xFAboru \u201E.wasm\u201C.
    Pr\xEDstup k na\u010D\xEDtaniu bol pravdepodobne zablokovan\xFD politikou CORS.
    Ak ste spr\xE1vcom servera, pomoc n\xE1jdete na Ruffle wiki.
error-wasm-invalid =
    Ruffle narazil na probl\xE9m pri pokuse o inicializ\xE1ciu.
    Zd\xE1 sa, \u017Ee na tejto str\xE1nke ch\xFDbaj\xFA alebo s\xFA neplatn\xE9 s\xFAbory na spustenie Ruffle.
    Ak ste spr\xE1vcom servera, pomoc n\xE1jdete na Ruffle wiki.
error-wasm-download =
    Ruffle narazil na probl\xE9m pri pokuse o inicializ\xE1ciu.
    Probl\xE9m sa m\xF4\u017Ee vyrie\u0161i\u0165 aj s\xE1m, tak\u017Ee m\xF4\u017Eete sk\xFAsi\u0165 str\xE1nku na\u010D\xEDta\u0165 znova.
    V opa\u010Dnom pr\xEDpade kontaktujte administr\xE1tora str\xE1nky.
error-wasm-disabled-on-edge =
    Ruffle sa nepodarilo na\u010D\xEDta\u0165 po\u017Eadovan\xFD komponent s\xFAboru \u201E.wasm\u201C.
    Ak chcete tento probl\xE9m vyrie\u0161i\u0165, sk\xFAste otvori\u0165 nastavenia prehliada\u010Da, kliknite na polo\u017Eku \u201EOchrana osobn\xFDch \xFAdajov, vyh\u013Ead\xE1vanie a slu\u017Eby\u201C, prejdite nadol a vypnite mo\u017Enos\u0165 \u201EZv\xFD\u0161te svoju bezpe\u010Dnos\u0165 na webe\u201C.
    V\xE1\u0161mu prehliada\u010Du to umo\u017En\xED na\u010D\xEDta\u0165 po\u017Eadovan\xE9 s\xFAbory \u201E.wasm\u201C.
    Ak probl\xE9m pretrv\xE1va, mo\u017Eno budete musie\u0165 pou\u017Ei\u0165 in\xFD prehliada\u010D.
error-javascript-conflict =
    Ruffle narazil na probl\xE9m pri pokuse o inicializ\xE1ciu.
    Zd\xE1 sa, \u017Ee t\xE1to str\xE1nka pou\u017E\xEDva k\xF3d JavaScript, ktor\xFD je v konflikte s Ruffle.
    Ak ste spr\xE1vcom servera, odpor\xFA\u010Dame v\xE1m sk\xFAsi\u0165 na\u010D\xEDta\u0165 s\xFAbor na pr\xE1zdnu str\xE1nku.
error-javascript-conflict-outdated = M\xF4\u017Eete sa tie\u017E pok\xFAsi\u0165 nahra\u0165 nov\u0161iu verziu Ruffle, ktor\xE1 m\xF4\u017Ee dan\xFD probl\xE9m vyrie\u0161i\u0165 (aktu\xE1lny build je zastaran\xFD: { $buildDate }).
error-csp-conflict =
    Ruffle narazil na probl\xE9m pri pokuse o inicializ\xE1ciu.
    Z\xE1sady zabezpe\u010Denia obsahu tohto webov\xE9ho servera nepovo\u013Euj\xFA spustenie po\u017Eadovan\xE9ho komponentu \u201E.wasm\u201C.
    Ak ste spr\xE1vcom servera, pomoc n\xE1jdete na Ruffle wiki.
error-unknown =
    Ruffle narazil na probl\xE9m pri pokuse zobrazi\u0165 tento Flash obsah.
    { $outdated ->
         [true] Ak ste spr\xE1vcom servera, sk\xFAste nahra\u0165 nov\u0161iu verziu Ruffle (aktu\xE1lny build je zastaran\xFD: { $buildDate }).
        *[false] Toto by sa nemalo sta\u0165, tak\u017Ee by sme naozaj ocenili, keby ste mohli nahl\xE1si\u0165 chybu!
    }
`,"save-manager.ftl":`save-delete-prompt = Naozaj chcete odstr\xE1ni\u0165 tento s\xFAbor s ulo\u017Een\xFDmi poz\xEDciami?
save-reload-prompt =
    Jedin\xFD sp\xF4sob, ako { $action ->
         [delete] vymaza\u0165
        *[replace] nahradi\u0165
    } tento s\xFAbor s ulo\u017Een\xFDmi poz\xEDciami bez potenci\xE1lneho konfliktu je op\xE4tovn\xE9 na\u010D\xEDtanie tohto obsahu. Chcete napriek tomu pokra\u010Dova\u0165?
save-download = Stiahnu\u0165
save-replace = Nahradi\u0165
save-delete = Vymaza\u0165
save-backup-all = Stiahnu\u0165 v\u0161etky s\xFAbory s ulo\u017Een\xFDmi poz\xEDciami
`,"volume-controls.ftl":`volume-controls = Ovl\xE1danie hlasitosti
volume-controls-mute = Stlmi\u0165
volume-controls-volume = Hlasitos\u0165
`},"sv-SE":{"context_menu.ftl":`context-menu-download-swf = Ladda ner .swf
context-menu-copy-debug-info = Kopiera fels\xF6kningsinfo
context-menu-open-save-manager = \xD6ppna Sparhanteraren
context-menu-about-ruffle =
    { $flavor ->
        [extension] Om Ruffletill\xE4gget ({ $version })
       *[other] Om Ruffle ({ $version })
    }
context-menu-hide = D\xF6lj denna meny
context-menu-exit-fullscreen = Avsluta helsk\xE4rm
context-menu-enter-fullscreen = Helsk\xE4rm
context-menu-volume-controls = Ljudkontroller
`,"messages.ftl":`message-cant-embed =
    Ruffle kunde inte k\xF6ra Flashinneh\xE5llet som \xE4r inb\xE4ddad p\xE5 denna sida.
    Du kan f\xF6rs\xF6ka \xF6ppna filen i en separat flik f\xF6r att kringg\xE5 problemet.
panic-title = N\xE5got gick fel :(
more-info = Mer info
run-anyway = K\xF6r \xE4nd\xE5
continue = Forts\xE4tt
report-bug = Rapportera Bugg
update-ruffle = Uppdatera Ruffle
ruffle-demo = Webbdemo
ruffle-desktop = Skrivbordsprogram
ruffle-wiki = Se Rufflewiki
enable-hardware-acceleration = Det verkar som att h\xE5rdvaruacceleration inte \xE4r p\xE5. Ruffle kan fortfarande fungera men kan vara orimligt l\xE5ngsam. Du kan ta reda p\xE5 hur man s\xE4tter p\xE5 h\xE5rdvaruacceleration genom att f\xF6lja denna l\xE4nk.
view-error-details = Visa Felinformation
open-in-new-tab = \xD6ppna i ny flik
click-to-unmute = Klicka f\xF6r ljud
error-file-protocol =
    Det verkar som att du k\xF6r Ruffle p\xE5 "fil:"-protokollet.
    Detta fungerar inte eftersom webbl\xE4sare blockerar m\xE5nga funktioner fr\xE5n att fungera av s\xE4kerhetssk\xE4l.
    Ist\xE4llet bjuder vi in dig att s\xE4tta upp en lokal server eller antingen anv\xE4nda webbdemon eller skrivbordsprogrammet.
error-javascript-config =
    Ruffle har st\xF6tt p\xE5 ett stort fel p\xE5 grund av en felaktig JavaScript-konfiguration.
    Om du \xE4r serveradministrat\xF6ren bjuder vi in dig att kontrollera feldetaljerna f\xF6r att ta reda p\xE5 vilken parameter som \xE4r felaktig.
    Du kan ocks\xE5 konsultera Rufflewikin f\xF6r hj\xE4lp.
error-wasm-not-found =
    Ruffle misslyckades ladda ".wasm"-filkomponenten.
    Om du \xE4r serveradministrat\xF6ren se till att filen har laddats upp korrekt.
    Om problemet kvarst\xE5r kan du beh\xF6va anv\xE4nda inst\xE4llningen "publicPath": konsultera v\xE4nligen Rufflewikin f\xF6r hj\xE4lp.
error-wasm-mime-type =
    Ruffle har st\xF6tt p\xE5 ett stort fel under initialiseringen.
    Denna webbserver serverar inte ".wasm"-filer med korrekt MIME-typ.
    Om du \xE4r serveradministrat\xF6ren konsultera v\xE4nligen Rufflewikin f\xF6r hj\xE4lp.
error-swf-fetch =
    Ruffle misslyckades ladda SWF-filen.
    Det mest sannolika sk\xE4let \xE4r att filen inte l\xE4ngre existerar, s\xE5 det finns inget f\xF6r Ruffle att k\xF6ra.
    F\xF6rs\xF6k att kontakta webbplatsadministrat\xF6ren f\xF6r hj\xE4lp.
error-swf-cors =
    Ruffle misslyckades ladda SWF-filen.
    \xC5tkomst att h\xE4mta har sannolikt blockerats av CORS-policy.
    Om du \xE4r serveradministrat\xF6ren konsultera v\xE4nligen Rufflewikin f\xF6r hj\xE4lp.
error-wasm-cors =
    Ruffle misslyckades ladda ".wasm"-filkomponenten.
    \xC5tkomst att h\xE4mta har sannolikt blockerats av CORS-policy.
    Om du \xE4r serveradministrat\xF6ren konsultera v\xE4nligen Rufflewikin f\xF6r hj\xE4lp.
error-wasm-invalid =
    Ruffle har st\xF6tt p\xE5 ett stort fel under initialiseringen.
    Det verkar som att den h\xE4r sidan har saknade eller ogiltiga filer f\xF6r att k\xF6ra Ruffle.
    Om du \xE4r serveradministrat\xF6ren konsultera v\xE4nligen Rufflewikin f\xF6r hj\xE4lp.
error-wasm-download =
    Ruffle har st\xF6tt p\xE5 ett stort fel under initialiseringen.
    Detta kan ofta l\xF6sas av sig sj\xE4lv s\xE5 du kan prova att ladda om sidan.
    Kontakta annars v\xE4nligen webbplatsens administrat\xF6r.
error-wasm-disabled-on-edge =
    Ruffle misslyckades ladda ".wasm"-filkomponenten.
    F\xF6r att \xE5tg\xE4rda detta f\xF6rs\xF6k att \xF6ppna webbl\xE4sarens inst\xE4llningar, klicka p\xE5 "Sekretess, s\xF6kning och tj\xE4nster", bl\xE4ddra ner och st\xE4ng av "F\xF6rb\xE4ttra s\xE4kerheten p\xE5 webben".
    Detta till\xE5ter din webbl\xE4sare ladda ".wasm"-filerna.
    Om problemet kvarst\xE5r kan du beh\xF6va anv\xE4nda en annan webbl\xE4sare.
error-javascript-conflict =
    Ruffle har st\xF6tt p\xE5 ett stort fel under initialiseringen.
    Det verkar som att den h\xE4r sidan anv\xE4nder JavaScript-kod som st\xF6r Ruffle.
    Om du \xE4r serveradministrat\xF6ren bjuder vi in dig att f\xF6rs\xF6ka ladda filen p\xE5 en blank sida.
error-javascript-conflict-outdated = Du kan ocks\xE5 f\xF6rs\xF6ka ladda upp en nyare version av Ruffle, vilket kan kringg\xE5 problemet (nuvarande version \xE4r utdaterad: { $buildDate }).
error-csp-conflict =
    Ruffle har st\xF6tt p\xE5 ett stort fel under initialiseringen.
    Denna webbservers Content Security Policy till\xE5ter inte ".wasm"-komponenten att k\xF6ra.
    Om du \xE4r serveradministrat\xF6ren konsultera v\xE4nligen Rufflewikin f\xF6r hj\xE4lp.
error-unknown =
    Ruffle har st\xF6tt p\xE5 ett stort fel medan den f\xF6rs\xF6kte visa Flashinneh\xE5llet.
    { $outdated ->
        [true] Om du \xE4r serveradministrat\xF6ren f\xF6rs\xF6k att ladda upp en nyare version av Ruffle (nuvarande version \xE4r utdaterad: { $buildDate }).
       *[false] Detta \xE4r inte t\xE4nkt att h\xE4nda s\xE5 vi skulle verkligen uppskatta om du kunde rapportera in en bugg!
    }
`,"save-manager.ftl":`save-delete-prompt = \xC4r du s\xE4ker p\xE5 att du vill radera sparfilen?
save-reload-prompt =
    Det enda s\xE4ttet att { $action ->
        [delete] radera
       *[replace] ers\xE4tta
    } denna sparfil utan potentiell konflikt \xE4r att ladda om inneh\xE5llet. Vill du forts\xE4tta \xE4nd\xE5?
save-download = Ladda ner
save-replace = Ers\xE4tt
save-delete = Radera
save-backup-all = Ladda ner alla sparfiler
`,"volume-controls.ftl":`volume-controls = Ljudkontroller
volume-controls-mute = St\xE4ng av ljud
volume-controls-volume = Volym
`},"tr-TR":{"context_menu.ftl":`context-menu-download-swf = \u0130ndir .swf
context-menu-copy-debug-info = Hata ay\u0131klama bilgisini kopyala
context-menu-open-save-manager = Kay\u0131t Y\xF6neticisini A\xE7
context-menu-about-ruffle =
    { $flavor ->
        [extension] Ruffle Uzant\u0131s\u0131 Hakk\u0131nda ({ $version })
       *[other] Ruffle Hakk\u0131nda ({ $version })
    }
context-menu-hide = Bu men\xFCy\xFC gizle
context-menu-exit-fullscreen = Tam ekrandan \xE7\u0131k
context-menu-enter-fullscreen = Tam ekran yap
`,"messages.ftl":`message-cant-embed =
    Ruffle, bu sayfaya g\xF6m\xFCl\xFC Flash'\u0131 \xE7al\u0131\u015Ft\u0131ramad\u0131.
    Bu sorunu ortadan kald\u0131rmak i\xE7in dosyay\u0131 ayr\u0131 bir sekmede a\xE7may\u0131 deneyebilirsiniz.
panic-title = Bir \u015Feyler yanl\u0131\u015F gitti :(
more-info = Daha fazla bilgi
run-anyway = Yine de \xE7al\u0131\u015Ft\u0131r
continue = Devam et
report-bug = Hata Bildir
update-ruffle = Ruffle'\u0131 G\xFCncelle
ruffle-demo = A\u011F Demosu
ruffle-desktop = Masa\xFCst\xFC Uygulamas\u0131
ruffle-wiki = Ruffle Wiki'yi G\xF6r\xFCnt\xFCle
view-error-details = Hata Ayr\u0131nt\u0131lar\u0131n\u0131 G\xF6r\xFCnt\xFCle
open-in-new-tab = Yeni sekmede a\xE7
click-to-unmute = Sesi a\xE7mak i\xE7in t\u0131klay\u0131n
error-file-protocol =
    G\xF6r\xFCn\xFC\u015Fe g\xF6re Ruffle'\u0131 "dosya:" protokol\xFCnde \xE7al\u0131\u015Ft\u0131r\u0131yorsunuz.
    Taray\u0131c\u0131lar g\xFCvenlik nedenleriyle bir\xE7ok \xF6zelli\u011Fin \xE7al\u0131\u015Fmas\u0131n\u0131 engelledi\u011Finden bu i\u015Fe yaramaz.
    Bunun yerine, sizi yerel bir sunucu kurmaya veya a\u011F\u0131n demosunu ya da masa\xFCst\xFC uygulamas\u0131n\u0131 kullanmaya davet ediyoruz.
error-javascript-config =
    Ruffle, yanl\u0131\u015F bir JavaScript yap\u0131land\u0131rmas\u0131 nedeniyle \xF6nemli bir sorunla kar\u015F\u0131la\u015Ft\u0131.
    Sunucu y\xF6neticisiyseniz, hangi parametrenin hatal\u0131 oldu\u011Funu bulmak i\xE7in sizi hata ayr\u0131nt\u0131lar\u0131n\u0131 kontrol etmeye davet ediyoruz.
    Yard\u0131m i\xE7in Ruffle wiki'sine de ba\u015Fvurabilirsiniz.
error-wasm-not-found =
    Ruffle gerekli ".wasm" dosya bile\u015Fenini y\xFCkleyemedi.
    Sunucu y\xF6neticisi iseniz, l\xFCtfen dosyan\u0131n do\u011Fru bir \u015Fekilde y\xFCklendi\u011Finden emin olun.
    Sorun devam ederse, "publicPath" ayar\u0131n\u0131 kullanman\u0131z gerekebilir: yard\u0131m i\xE7in l\xFCtfen Ruffle wiki'sine ba\u015Fvurun.
error-wasm-mime-type =
    Ruffle, ba\u015Flatmaya \xE7al\u0131\u015F\u0131rken \xF6nemli bir sorunla kar\u015F\u0131la\u015Ft\u0131.
    Bu web sunucusu, do\u011Fru MIME tipinde ".wasm" dosyalar\u0131 sunmuyor.
    Sunucu y\xF6neticisiyseniz, yard\u0131m i\xE7in l\xFCtfen Ruffle wiki'sine ba\u015Fvurun.
error-swf-fetch =
    Ruffle, Flash SWF dosyas\u0131n\u0131 y\xFCkleyemedi.
    Bunun en olas\u0131 nedeni, dosyan\u0131n art\u0131k mevcut olmamas\u0131 ve bu nedenle Ruffle'\u0131n y\xFCkleyece\u011Fi hi\xE7bir \u015Feyin olmamas\u0131d\u0131r.
    Yard\u0131m i\xE7in web sitesi y\xF6neticisiyle ileti\u015Fime ge\xE7meyi deneyin.
error-swf-cors =
    Ruffle, Flash SWF dosyas\u0131n\u0131 y\xFCkleyemedi.
    Getirme eri\u015Fimi muhtemelen CORS politikas\u0131 taraf\u0131ndan engellenmi\u015Ftir.
    Sunucu y\xF6neticisiyseniz, yard\u0131m i\xE7in l\xFCtfen Ruffle wiki'sine ba\u015Fvurun.
error-wasm-cors =
    Ruffle gerekli ".wasm" dosya bile\u015Fenini y\xFCkleyemedi.
    Getirme eri\u015Fimi muhtemelen CORS politikas\u0131 taraf\u0131ndan engellenmi\u015Ftir.
    Sunucu y\xF6neticisiyseniz, yard\u0131m i\xE7in l\xFCtfen Ruffle wiki'sine ba\u015Fvurun.
error-wasm-invalid =
    Ruffle, ba\u015Flatmaya \xE7al\u0131\u015F\u0131rken \xF6nemli bir sorunla kar\u015F\u0131la\u015Ft\u0131.
    G\xF6r\xFCn\xFC\u015Fe g\xF6re bu sayfada Ruffle'\u0131 \xE7al\u0131\u015Ft\u0131rmak i\xE7in eksik veya ge\xE7ersiz dosyalar var.
    Sunucu y\xF6neticisiyseniz, yard\u0131m i\xE7in l\xFCtfen Ruffle wiki'sine ba\u015Fvurun.
error-wasm-download =
    Ruffle, ba\u015Flatmaya \xE7al\u0131\u015F\u0131rken \xF6nemli bir sorunla kar\u015F\u0131la\u015Ft\u0131.
    Bu genellikle kendi kendine \xE7\xF6z\xFClebilir, bu nedenle sayfay\u0131 yeniden y\xFCklemeyi deneyebilirsiniz.
    Aksi takdirde, l\xFCtfen site y\xF6neticisiyle ileti\u015Fime ge\xE7in.
error-wasm-disabled-on-edge =
    Ruffle gerekli ".wasm" dosya bile\u015Fenini y\xFCkleyemedi.
    Bunu d\xFCzeltmek i\xE7in taray\u0131c\u0131n\u0131z\u0131n ayarlar\u0131n\u0131 a\xE7\u0131n, "Gizlilik, arama ve hizmetler"i t\u0131klay\u0131n, a\u015Fa\u011F\u0131 kayd\u0131r\u0131n ve "Web'de g\xFCvenli\u011Finizi art\u0131r\u0131n"\u0131 kapatmay\u0131 deneyin.
    Bu, taray\u0131c\u0131n\u0131z\u0131n gerekli ".wasm" dosyalar\u0131n\u0131 y\xFCklemesine izin verecektir.
    Sorun devam ederse, farkl\u0131 bir taray\u0131c\u0131 kullanman\u0131z gerekebilir.
error-javascript-conflict =
    Ruffle, ba\u015Flatmaya \xE7al\u0131\u015F\u0131rken \xF6nemli bir sorunla kar\u015F\u0131la\u015Ft\u0131.
    G\xF6r\xFCn\xFC\u015Fe g\xF6re bu sayfa, Ruffle ile \xE7ak\u0131\u015Fan JavaScript kodu kullan\u0131yor.
    Sunucu y\xF6neticisiyseniz, sizi dosyay\u0131 bo\u015F bir sayfaya y\xFCklemeyi denemeye davet ediyoruz.
error-javascript-conflict-outdated = Ayr\u0131ca sorunu giderebilecek daha yeni bir Ruffle s\xFCr\xFCm\xFC y\xFCklemeyi de deneyebilirsiniz (mevcut yap\u0131m eskimi\u015F: { $buildDate }).
error-csp-conflict =
    Ruffle, ba\u015Flatmaya \xE7al\u0131\u015F\u0131rken \xF6nemli bir sorunla kar\u015F\u0131la\u015Ft\u0131.
    Bu web sunucusunun \u0130\xE7erik G\xFCvenli\u011Fi Politikas\u0131, gerekli ".wasm" bile\u015Feninin \xE7al\u0131\u015Fmas\u0131na izin vermiyor.
    Sunucu y\xF6neticisiyseniz, yard\u0131m i\xE7in l\xFCtfen Ruffle wiki'sine bak\u0131n.
error-unknown =
    Ruffle, bu Flash i\xE7eri\u011Fini g\xF6r\xFCnt\xFClemeye \xE7al\u0131\u015F\u0131rken \xF6nemli bir sorunla kar\u015F\u0131la\u015Ft\u0131.
    { $outdated ->
        [true] Sunucu y\xF6neticisiyseniz, l\xFCtfen Ruffle'\u0131n daha yeni bir s\xFCr\xFCm\xFCn\xFC y\xFCklemeyi deneyin (mevcut yap\u0131m eskimi\u015F: { $buildDate }).
       *[false] Bunun olmamas\u0131 gerekiyor, bu y\xFCzden bir hata bildirebilirseniz \xE7ok memnun oluruz!
    }
`,"save-manager.ftl":`save-delete-prompt = Bu kay\u0131t dosyas\u0131n\u0131 silmek istedi\u011Finize emin misiniz?
save-reload-prompt =
    Bu kaydetme dosyas\u0131n\u0131 potansiyel \xE7ak\u0131\u015Fma olmadan { $action ->
        [delete] silmenin
       *[replace] de\u011Fi\u015Ftirmenin
    } tek yolu, bu i\xE7eri\u011Fi yeniden y\xFCklemektir. Yine de devam etmek istiyor musunuz?
save-download = \u0130ndir
save-replace = De\u011Fi\u015Ftir
save-delete = Sil
save-backup-all = T\xFCm kay\u0131t dosyalar\u0131n\u0131 indir
`,"volume-controls.ftl":""},"zh-CN":{"context_menu.ftl":`context-menu-download-swf = \u4E0B\u8F7D .swf
context-menu-copy-debug-info = \u590D\u5236\u8C03\u8BD5\u4FE1\u606F
context-menu-open-save-manager = \u6253\u5F00\u5B58\u6863\u7BA1\u7406\u5668
context-menu-about-ruffle =
    { $flavor ->
        [extension] \u5173\u4E8E Ruffle \u6269\u5C55 ({ $version })
       *[other] \u5173\u4E8E Ruffle ({ $version })
    }
context-menu-hide = \u9690\u85CF\u6B64\u83DC\u5355
context-menu-exit-fullscreen = \u9000\u51FA\u5168\u5C4F
context-menu-enter-fullscreen = \u8FDB\u5165\u5168\u5C4F
context-menu-volume-controls = \u97F3\u91CF\u63A7\u5236
`,"messages.ftl":`message-cant-embed =
    Ruffle \u65E0\u6CD5\u8FD0\u884C\u5D4C\u5165\u5728\u6B64\u9875\u9762\u4E2D\u7684 Flash\u3002
    \u60A8\u53EF\u4EE5\u5C1D\u8BD5\u5728\u5355\u72EC\u7684\u6807\u7B7E\u9875\u4E2D\u6253\u5F00\u8BE5\u6587\u4EF6\uFF0C\u4EE5\u56DE\u907F\u6B64\u95EE\u9898\u3002
panic-title = \u51FA\u4E86\u4E9B\u95EE\u9898 :(
more-info = \u66F4\u591A\u4FE1\u606F
run-anyway = \u4ECD\u7136\u8FD0\u884C
continue = \u7EE7\u7EED
report-bug = \u53CD\u9988\u95EE\u9898
update-ruffle = \u66F4\u65B0 Ruffle
ruffle-demo = \u7F51\u9875\u6F14\u793A
ruffle-desktop = \u684C\u9762\u5E94\u7528\u7A0B\u5E8F
ruffle-wiki = \u67E5\u770B Ruffle Wiki
enable-hardware-acceleration = \u770B\u8D77\u6765\u786C\u4EF6\u52A0\u901F\u672A\u542F\u7528\u3002\u867D\u7136 Ruffle \u53EF\u80FD\u8FD0\u884C\uFF0C\u4F46\u53EF\u80FD\u4F1A\u975E\u5E38\u6162\u3002\u60A8\u53EF\u4EE5\u901A\u8FC7\u6B64\u94FE\u63A5\u4E86\u89E3\u542F\u7528\u786C\u4EF6\u52A0\u901F\u7684\u65B9\u6CD5\u3002
view-error-details = \u67E5\u770B\u9519\u8BEF\u8BE6\u60C5
open-in-new-tab = \u5728\u65B0\u6807\u7B7E\u9875\u4E2D\u6253\u5F00
click-to-unmute = \u70B9\u51FB\u53D6\u6D88\u9759\u97F3
error-file-protocol =
    \u770B\u6765\u60A8\u6B63\u5728 "file:" \u534F\u8BAE\u4E0A\u4F7F\u7528 Ruffle\u3002
    \u7531\u4E8E\u6D4F\u89C8\u5668\u4EE5\u5B89\u5168\u539F\u56E0\u963B\u6B62\u8BB8\u591A\u529F\u80FD\uFF0C\u56E0\u6B64\u8FD9\u4E0D\u8D77\u4F5C\u7528\u3002
    \u76F8\u53CD\u6211\u4EEC\u9080\u8BF7\u60A8\u8BBE\u7F6E\u672C\u5730\u670D\u52A1\u5668\u6216\u4F7F\u7528\u7F51\u9875\u6F14\u793A\u6216\u684C\u9762\u5E94\u7528\u7A0B\u5E8F\u3002
error-javascript-config =
    \u7531\u4E8E\u9519\u8BEF\u7684 JavaScript \u914D\u7F6E\uFF0CRuffle \u9047\u5230\u4E86\u4E00\u4E2A\u91CD\u5927\u95EE\u9898\u3002
    \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u6211\u4EEC\u9080\u8BF7\u60A8\u68C0\u67E5\u9519\u8BEF\u8BE6\u7EC6\u4FE1\u606F\uFF0C\u4EE5\u627E\u51FA\u54EA\u4E2A\u53C2\u6570\u6709\u6545\u969C\u3002
    \u60A8\u4E5F\u53EF\u4EE5\u67E5\u9605 Ruffle \u7684 Wiki \u83B7\u53D6\u5E2E\u52A9\u3002
error-wasm-not-found =
    Ruffle \u65E0\u6CD5\u52A0\u8F7D\u6240\u9700\u7684 \u201C.wasm\u201D \u6587\u4EF6\u7EC4\u4EF6\u3002
    \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u8BF7\u786E\u4FDD\u6587\u4EF6\u5DF2\u6B63\u786E\u4E0A\u4F20\u3002
    \u5982\u679C\u95EE\u9898\u4ECD\u7136\u5B58\u5728\uFF0C\u60A8\u53EF\u80FD\u9700\u8981\u4F7F\u7528 \u201CpublicPath\u201D \u8BBE\u7F6E\uFF1A\u8BF7\u67E5\u770B Ruffle \u7684 Wiki \u83B7\u53D6\u5E2E\u52A9\u3002
error-wasm-mime-type =
    Ruffle \u5728\u8BD5\u56FE\u521D\u59CB\u5316\u65F6\u9047\u5230\u4E86\u4E00\u4E2A\u91CD\u5927\u95EE\u9898\u3002
    \u8BE5\u7F51\u7AD9\u670D\u52A1\u5668\u6CA1\u6709\u63D0\u4F9B ".asm\u201D \u6587\u4EF6\u6B63\u786E\u7684 MIME \u7C7B\u578B\u3002
    \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u8BF7\u67E5\u9605 Ruffle Wiki \u83B7\u53D6\u5E2E\u52A9\u3002
error-swf-fetch =
    Ruffle \u65E0\u6CD5\u52A0\u8F7D Flash SWF \u6587\u4EF6\u3002
    \u6700\u53EF\u80FD\u7684\u539F\u56E0\u662F\u6587\u4EF6\u4E0D\u518D\u5B58\u5728\u6240\u4EE5 Ruffle \u6CA1\u6709\u8981\u52A0\u8F7D\u7684\u5185\u5BB9\u3002
    \u8BF7\u5C1D\u8BD5\u8054\u7CFB\u7F51\u7AD9\u7BA1\u7406\u5458\u5BFB\u6C42\u5E2E\u52A9\u3002
error-swf-cors =
    Ruffle \u65E0\u6CD5\u52A0\u8F7D Flash SWF \u6587\u4EF6\u3002
    \u83B7\u53D6\u6743\u9650\u53EF\u80FD\u88AB CORS \u7B56\u7565\u963B\u6B62\u3002
    \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u8BF7\u53C2\u8003 Ruffle Wiki \u83B7\u53D6\u5E2E\u52A9\u3002
error-wasm-cors =
    Ruffle \u65E0\u6CD5\u52A0\u8F7D\u6240\u9700\u7684\u201C.wasm\u201D\u6587\u4EF6\u7EC4\u4EF6\u3002
    \u83B7\u53D6\u6743\u9650\u53EF\u80FD\u88AB CORS \u7B56\u7565\u963B\u6B62\u3002
    \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u8BF7\u67E5\u9605 Ruffle Wiki \u83B7\u53D6\u5E2E\u52A9\u3002
error-wasm-invalid =
    Ruffle \u5728\u8BD5\u56FE\u521D\u59CB\u5316\u65F6\u9047\u5230\u4E86\u4E00\u4E2A\u91CD\u5927\u95EE\u9898\u3002
    \u8FD9\u4E2A\u9875\u9762\u4F3C\u4E4E\u7F3A\u5C11\u6587\u4EF6\u6765\u8FD0\u884C Curl\u3002
    \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u8BF7\u67E5\u9605 Ruffle Wiki \u83B7\u53D6\u5E2E\u52A9\u3002
error-wasm-download =
    Ruffle \u5728\u8BD5\u56FE\u521D\u59CB\u5316\u65F6\u9047\u5230\u4E86\u4E00\u4E2A\u91CD\u5927\u95EE\u9898\u3002
    \u8FD9\u901A\u5E38\u53EF\u4EE5\u81EA\u884C\u89E3\u51B3\uFF0C\u56E0\u6B64\u60A8\u53EF\u4EE5\u5C1D\u8BD5\u91CD\u65B0\u52A0\u8F7D\u9875\u9762\u3002
    \u5426\u5219\u8BF7\u8054\u7CFB\u7F51\u7AD9\u7BA1\u7406\u5458\u3002
error-wasm-disabled-on-edge =
    Ruffle \u65E0\u6CD5\u52A0\u8F7D\u6240\u9700\u7684 \u201C.wasm\u201D \u6587\u4EF6\u7EC4\u4EF6\u3002
    \u8981\u89E3\u51B3\u8FD9\u4E2A\u95EE\u9898\uFF0C\u8BF7\u5C1D\u8BD5\u6253\u5F00\u60A8\u7684\u6D4F\u89C8\u5668\u8BBE\u7F6E\uFF0C\u5355\u51FB"\u9690\u79C1\u3001\u641C\u7D22\u548C\u670D\u52A1"\uFF0C\u5411\u4E0B\u6EDA\u52A8\u5E76\u5173\u95ED"\u589E\u5F3A Web \u5B89\u5168\u6027"\u3002
    \u8FD9\u5C06\u5141\u8BB8\u60A8\u7684\u6D4F\u89C8\u5668\u52A0\u8F7D\u6240\u9700\u7684 \u201C.wasm\u201D \u6587\u4EF6\u3002
    \u5982\u679C\u95EE\u9898\u4ECD\u7136\u5B58\u5728\uFF0C\u60A8\u53EF\u80FD\u5FC5\u987B\u4F7F\u7528\u4E0D\u540C\u7684\u6D4F\u89C8\u5668\u3002
error-javascript-conflict =
    Ruffle \u5728\u8BD5\u56FE\u521D\u59CB\u5316\u65F6\u9047\u5230\u4E86\u4E00\u4E2A\u91CD\u5927\u95EE\u9898\u3002
    \u8FD9\u4E2A\u9875\u9762\u4F3C\u4E4E\u4F7F\u7528\u4E86\u4E0E Ruffle \u51B2\u7A81\u7684 JavaScript \u4EE3\u7801\u3002
    \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u6211\u4EEC\u5EFA\u8BAE\u60A8\u5C1D\u8BD5\u5728\u7A7A\u767D\u9875\u9762\u4E0A\u52A0\u8F7D\u6587\u4EF6\u3002
error-javascript-conflict-outdated = \u60A8\u8FD8\u53EF\u4EE5\u5C1D\u8BD5\u4E0A\u4F20\u53EF\u80FD\u89C4\u907F\u8BE5\u95EE\u9898\u7684\u6700\u65B0\u7248\u672C\u7684 (\u5F53\u524D\u6784\u5EFA\u5DF2\u8FC7\u65F6: { $buildDate })\u3002
error-csp-conflict =
    Ruffle \u5728\u8BD5\u56FE\u521D\u59CB\u5316\u65F6\u9047\u5230\u4E86\u4E00\u4E2A\u91CD\u5927\u95EE\u9898\u3002
    \u8BE5\u7F51\u7AD9\u670D\u52A1\u5668\u7684\u5185\u5BB9\u5B89\u5168\u7B56\u7565\u4E0D\u5141\u8BB8\u8FD0\u884C\u6240\u9700\u7684 \u201C.wasm\u201D \u7EC4\u4EF6\u3002
    \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u8BF7\u67E5\u9605 Ruffle Wiki \u83B7\u53D6\u5E2E\u52A9\u3002
error-unknown =
    Ruffle \u5728\u8BD5\u56FE\u663E\u793A\u6B64 Flash \u5185\u5BB9\u65F6\u9047\u5230\u4E86\u4E00\u4E2A\u91CD\u5927\u95EE\u9898\u3002
    { $outdated ->
        [true] \u5982\u679C\u60A8\u662F\u670D\u52A1\u5668\u7BA1\u7406\u5458\uFF0C\u8BF7\u5C1D\u8BD5\u4E0A\u4F20\u66F4\u65B0\u7684 Ruffle \u7248\u672C (\u5F53\u524D\u7248\u672C\u5DF2\u8FC7\u65F6: { $buildDate }).
       *[false] \u8FD9\u4E0D\u5E94\u8BE5\u53D1\u751F\uFF0C\u56E0\u6B64\u5982\u679C\u60A8\u53EF\u4EE5\u62A5\u544A\u9519\u8BEF\uFF0C\u6211\u4EEC\u5C06\u975E\u5E38\u611F\u8C22\uFF01
    }
`,"save-manager.ftl":`save-delete-prompt = \u786E\u5B9A\u8981\u5220\u9664\u6B64\u5B58\u6863\u5417\uFF1F
save-reload-prompt =
    \u4E3A\u4E86\u907F\u514D\u6F5C\u5728\u7684\u51B2\u7A81\uFF0C{ $action ->
        [delete] \u5220\u9664
       *[replace] \u66FF\u6362
    } \u6B64\u5B58\u6863\u6587\u4EF6\u9700\u8981\u91CD\u65B0\u52A0\u8F7D\u5F53\u524D\u5185\u5BB9\u3002\u662F\u5426\u4ECD\u7136\u7EE7\u7EED\uFF1F
save-download = \u4E0B\u8F7D
save-replace = \u66FF\u6362
save-delete = \u5220\u9664
save-backup-all = \u4E0B\u8F7D\u6240\u6709\u5B58\u6863\u6587\u4EF6
`,"volume-controls.ftl":`volume-controls = \u97F3\u91CF\u63A7\u5236
volume-controls-mute = \u9759\u97F3
volume-controls-volume = \u97F3\u91CF
`},"zh-TW":{"context_menu.ftl":`context-menu-download-swf = \u4E0B\u8F09SWF\u6A94\u6848
context-menu-copy-debug-info = \u8907\u88FD\u9664\u932F\u8CC7\u8A0A
context-menu-open-save-manager = \u6253\u958B\u5B58\u6A94\u7BA1\u7406\u5668
context-menu-about-ruffle =
    { $flavor ->
        [extension] \u95DC\u65BCRuffle\u64F4\u5145\u529F\u80FD ({ $version })
       *[other] \u95DC\u65BCRuffle ({ $version })
    }
context-menu-hide = \u96B1\u85CF\u83DC\u55AE
context-menu-exit-fullscreen = \u9000\u51FA\u5168\u87A2\u5E55
context-menu-enter-fullscreen = \u9032\u5165\u5168\u87A2\u5E55
context-menu-volume-controls = \u97F3\u91CF\u63A7\u5236
`,"messages.ftl":`message-cant-embed =
    \u76EE\u524DRuffle\u6C92\u8FA6\u6CD5\u57F7\u884C\u5D4C\u5165\u5F0FFlash\u3002
    \u4F60\u53EF\u4EE5\u5728\u65B0\u5206\u9801\u4E2D\u958B\u555F\u4F86\u89E3\u6C7A\u9019\u500B\u554F\u984C\u3002
panic-title = \u5B8C\u86CB\uFF0C\u51FA\u554F\u984C\u4E86 :(
more-info = \u66F4\u591A\u8CC7\u8A0A
run-anyway = \u76F4\u63A5\u57F7\u884C
continue = \u7E7C\u7E8C
report-bug = \u56DE\u5831BUG
update-ruffle = \u66F4\u65B0Ruffle
ruffle-demo = \u7DB2\u9801\u5C55\u793A
ruffle-desktop = \u684C\u9762\u61C9\u7528\u7A0B\u5F0F
ruffle-wiki = \u67E5\u770BRuffle Wiki
enable-hardware-acceleration =
    \u770B\u8D77\u4F86\u4F60\u7684\u786C\u9AD4\u52A0\u901F\u6C92\u6709\u958B\u555F\uFF0C\u96D6\u7136Ruffle\u9084\u53EF\u4EE5\u57F7\u884C\uFF0C\u4F46\u662F\u4F60\u6703\u611F\u89BA\u5230\u6703\u5F88\u6162\u3002
    \u4F60\u53EF\u4EE5\u5728\u4E0B\u65B9\u9023\u7D50\u627E\u5230\u5982\u4F55\u958B\u555F\u786C\u9AD4\u52A0\u901F\u3002
view-error-details = \u6AA2\u8996\u932F\u8AA4\u8A73\u7D30\u8CC7\u6599
open-in-new-tab = \u958B\u555F\u65B0\u589E\u5206\u9801
click-to-unmute = \u9EDE\u64CA\u4EE5\u53D6\u6D88\u975C\u97F3
error-file-protocol =
    \u770B\u8D77\u4F86\u4F60\u60F3\u8981\u7528Ruffle\u4F86\u57F7\u884C"file:"\u7684\u5354\u8B70\u3002
    \u56E0\u70BA\u700F\u89BD\u5668\u7981\u4E86\u5F88\u591A\u529F\u80FD\u4EE5\u8CC7\u5B89\u7684\u7406\u7531\u4F86\u8B1B\u3002
    \u6211\u5011\u5EFA\u8B70\u4F60\u5EFA\u7ACB\u672C\u5730\u4F3A\u670D\u5668\u6216\u8457\u76F4\u63A5\u4F7F\u7528\u7DB2\u9801\u5C55\u793A\u6216\u684C\u9762\u61C9\u7528\u7A0B\u5F0F\u3002
error-javascript-config =
    \u76EE\u524DRuffle\u9047\u5230\u4E0D\u6B63\u78BA\u7684JavaScript\u914D\u7F6E\u3002
    \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C\u6211\u5011\u5EFA\u8B70\u4F60\u6AA2\u67E5\u54EA\u500B\u74B0\u7BC0\u51FA\u932F\u3002
    \u6216\u8457\u4F60\u53EF\u4EE5\u67E5\u8A62Ruffle wiki\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-wasm-not-found =
    \u76EE\u524DRuffle\u627E\u4E0D\u5230".wasm"\u6A94\u6848\u3002
    \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C\u78BA\u4FDD\u6A94\u6848\u662F\u5426\u653E\u5C0D\u4F4D\u7F6E\u3002
    \u5982\u679C\u9084\u662F\u6709\u554F\u984C\u7684\u8A71\uFF0C\u4F60\u8981\u7528"publicPath"\u4F86\u8A2D\u5B9A: \u6216\u8457\u67E5\u8A62Ruffle wiki\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-wasm-mime-type =
    \u76EE\u524DRuffle\u521D\u59CB\u5316\u6642\u9047\u5230\u91CD\u5927\u554F\u984C\u3002
    \u9019\u7DB2\u9801\u4F3A\u670D\u5668\u4E26\u6C92\u6709\u670D\u52D9".wasm"\u6A94\u6848\u6216\u6B63\u78BA\u7684\u7DB2\u969B\u7DB2\u8DEF\u5A92\u9AD4\u985E\u578B\u3002
    \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C\u8ACB\u67E5\u8A62Ruffle wiki\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-swf-fetch =
    \u76EE\u524DRuffle\u7121\u6CD5\u8B80\u53D6Flash\u7684SWF\u6A94\u6848\u3002
    \u5F88\u6709\u53EF\u80FD\u8981\u8B80\u53D6\u7684\u6A94\u6848\u4E0D\u5B58\u5728\uFF0C\u6240\u4EE5Ruffle\u8B80\u4E0D\u5230\u6771\u897F\u3002
    \u8ACB\u5617\u8A66\u6E9D\u901A\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-swf-cors =
    \u76EE\u524DRuffle\u7121\u6CD5\u8B80\u53D6Flash\u7684SWF\u6A94\u6848\u3002
    \u770B\u8D77\u4F86\u662F\u4F7F\u7528\u6B0A\u88AB\u8DE8\u4F86\u6E90\u8CC7\u6E90\u5171\u7528\u6A5F\u5236\u88AB\u64CB\u5230\u4E86\u3002
    \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C\u8ACB\u67E5\u8A62Ruffle wiki\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-wasm-cors =
    \u76EE\u524DRuffle\u7121\u6CD5\u8B80\u53D6".wasm"\u6A94\u6848\u3002
    \u770B\u8D77\u4F86\u662F\u4F7F\u7528\u6B0A\u88AB\u8DE8\u4F86\u6E90\u8CC7\u6E90\u5171\u7528\u6A5F\u5236\u88AB\u64CB\u5230\u4E86\u3002
    \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C\u8ACB\u67E5\u8A62Ruffle wiki\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-wasm-invalid =
    \u76EE\u524DRuffle\u521D\u59CB\u5316\u6642\u9047\u5230\u91CD\u5927\u554F\u984C\u3002
    \u770B\u8D77\u4F86\u9019\u7DB2\u9801\u6709\u7F3A\u5931\u6A94\u6848\u5C0E\u81F4Ruffle\u7121\u6CD5\u904B\u884C\u3002
    \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C\u8ACB\u67E5\u8A62Ruffle wiki\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-wasm-download =
    \u76EE\u524DRuffle\u521D\u59CB\u5316\u6642\u9047\u5230\u91CD\u5927\u554F\u984C\u3002
    \u9019\u53EF\u4EE5\u4F60\u81EA\u5DF1\u89E3\u6C7A\uFF0C\u4F60\u53EA\u8981\u91CD\u65B0\u6574\u7406\u5C31\u597D\u4E86\u3002
    \u5426\u5247\uFF0C\u8ACB\u5617\u8A66\u6E9D\u901A\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-wasm-disabled-on-edge =
    \u76EE\u524DRuffle\u7121\u6CD5\u8B80\u53D6".wasm"\u6A94\u6848\u3002
    \u8981\u4FEE\u6B63\u7684\u8A71\uFF0C\u6253\u958B\u4F60\u7684\u700F\u89BD\u5668\u8A2D\u5B9A\uFF0C\u9EDE\u9078"\u96B1\u79C1\u6B0A\u3001\u641C\u5C0B\u8207\u670D\u52D9"\uFF0C\u628A"\u9632\u6B62\u8FFD\u8E64"\u7D66\u95DC\u6389\u3002
    \u9019\u6A23\u4E00\u4F86\u4F60\u7684\u700F\u89BD\u5668\u6703\u8B80\u53D6\u9700\u8981\u7684".wasm"\u6A94\u6848\u3002
    \u5982\u679C\u554F\u984C\u4E00\u76F4\u9084\u5728\u7684\u8A71\uFF0C\u4F60\u5FC5\u9808\u8981\u63DB\u700F\u89BD\u5668\u4E86\u3002
error-javascript-conflict =
    \u76EE\u524DRuffle\u521D\u59CB\u5316\u6642\u9047\u5230\u91CD\u5927\u554F\u984C\u3002
    \u770B\u8D77\u4F86\u9019\u7DB2\u9801\u4F7F\u7528\u7684JavaScript\u6703\u8DDFRuffle\u8D77\u885D\u7A81\u3002
    \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C\u6211\u5011\u5EFA\u8B70\u4F60\u958B\u500B\u7A7A\u767D\u9801\u4F86\u6E2C\u8A66\u3002
error-javascript-conflict-outdated = \u4F60\u4E5F\u53EF\u4EE5\u4E0A\u50B3\u6700\u65B0\u7248\u7684Ruffle\uFF0C\u8AAA\u4E0D\u5B9A\u4F60\u8981\u8AAA\u7684\u7684\u554F\u984C\u5DF2\u7D93\u4E0D\u898B\u4E86(\u73FE\u5728\u4F7F\u7528\u7684\u7248\u672C\u5DF2\u7D93\u904E\u6642: { $buildDate })\u3002
error-csp-conflict =
    \u76EE\u524DRuffle\u521D\u59CB\u5316\u6642\u9047\u5230\u91CD\u5927\u554F\u984C\u3002
    \u9019\u7DB2\u9801\u4F3A\u670D\u5668\u88AB\u8DE8\u4F86\u6E90\u8CC7\u6E90\u5171\u7528\u6A5F\u5236\u7981\u6B62\u8B80\u53D6".wasm"\u6A94\u6848\u3002
    \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C\u8ACB\u67E5\u8A62Ruffle wiki\u5F97\u5230\u9700\u6C42\u5E6B\u52A9\u3002
error-unknown =
    \u76EE\u524DRuffle\u521D\u59CB\u5316\u8981\u8B80\u53D6Flash\u5167\u5BB9\u6642\u9047\u5230\u91CD\u5927\u554F\u984C
    { $outdated ->
        [true] \u5982\u679C\u4F60\u662F\u4F3A\u670D\u5668\u7BA1\u7406\u54E1\uFF0C \u8ACB\u4E0A\u50B3\u6700\u65B0\u7248\u7684Ruffle(\u73FE\u5728\u4F7F\u7528\u7684\u7248\u672C\u5DF2\u7D93\u904E\u6642: { $buildDate }).
       *[false] \u9019\u4E0D\u61C9\u8A72\u767C\u751F\u7684\uFF0C\u6211\u5011\u4E5F\u5F88\u9AD8\u8208\u4F60\u544A\u77E5bug!
    }
`,"save-manager.ftl":`save-delete-prompt = \u4F60\u78BA\u5B9A\u8981\u522A\u9664\u9019\u500B\u5B58\u6A94\u55CE\uFF1F
save-reload-prompt =
    \u552F\u4E00\u65B9\u6CD5\u53EA\u6709 { $action ->
        [delete] \u522A\u9664
       *[replace] \u53D6\u4EE3
    } \u9019\u500B\u5B58\u6A94\u4E0D\u6703\u5B8C\u5168\u53D6\u4EE3\u76F4\u5230\u91CD\u65B0\u555F\u52D5. \u4F60\u9700\u8981\u7E7C\u7E8C\u55CE?
save-download = \u4E0B\u8F09
save-replace = \u53D6\u4EE3
save-delete = \u522A\u9664
save-backup-all = \u4E0B\u8F09\u6240\u6709\u5B58\u6A94\u6A94\u6848\u3002
`,"volume-controls.ftl":`volume-controls = \u97F3\u91CF\u63A7\u5236
volume-controls-mute = \u975C\u97F3
volume-controls-volume = \u97F3\u91CF
`}},eu={};for(const[p,e]of Object.entries(Le)){const u=new Z(p);if(e){for(const[a,s]of Object.entries(e))if(s)for(const g of u.addResource(new I(s)))console.error(`Error in text for ${p} ${a}: ${g}`)}eu[p]=u}function Ue(p,e,u){const a=eu[p];if(a!==void 0){const s=a.getMessage(e);if(s!==void 0&&s.value)return a.formatPattern(s.value,u)}return null}function we(p,e){const u=Re(navigator.languages,Object.keys(eu),{defaultLocale:"en-US"});for(const a in u){const s=Ue(u[a],p,e);if(s)return s}return console.error(`Unknown text key '${p}'`),p}function ye(p,e){const u=document.createElement("div");return we(p,e).split(`
`).forEach((a=>{const s=document.createElement("p");s.innerText=a,u.appendChild(s)})),u}function re(p,e,u,a,s){const g=s?document.createElementNS(s,p):document.createElement(p);if(e&&(g.id=e),u&&s?g.classList.add(u):u&&(g.className=u),a)for(const[L,K]of Object.entries(a))g.setAttribute(L,K);return g}function ru(p,e,u,a,s){const g=re("input",e);return g.type=p,u&&(g.min=u),a&&(g.max=a),s&&(g.step=s),g}function Ze(p,e){const u=re("label",p);return u.htmlFor=e,u}function ae(p,e){p.appendChild(e)}const He=document.createElement("template"),Ae="http://www.w3.org/2000/svg",Au=re("style","static-styles"),vu=re("style","dynamic-styles"),Ye=re("div","container"),tu=re("div","play-button"),Qu=re("div",void 0,"icon"),Iu=re("svg",void 0,void 0,{xmlns:Ae,"xmlns:xlink":"http://www.w3.org/1999/xlink",preserveAspectRatio:"xMidYMid",viewBox:"0 0 250 250",width:"100%",height:"100%"},Ae),Ou=re("defs",void 0,void 0,void 0,Ae),Fu=re("linearGradient","a",void 0,{gradientUnits:"userSpaceOnUse",x1:"125",y1:"0",x2:"125",y2:"250",spreadMethod:"pad"},Ae),Fn=re("stop",void 0,void 0,{offset:"0%","stop-color":"#FDA138"},Ae),Dn=re("stop",void 0,void 0,{offset:"100%","stop-color":"#FD3A40"},Ae),Du=re("g","b",void 0,void 0,Ae),Pn=re("path",void 0,void 0,{fill:"url(#a)",d:"M250 125q0-52-37-88-36-37-88-37T37 37Q0 73 0 125t37 88q36 37 88 37t88-37q37-36 37-88M87 195V55l100 70-100 70z"},Ae),Tn=re("path",void 0,void 0,{fill:"#FFF",d:"M87 55v140l100-70L87 55z"},Ae),en=document.createElementNS(Ae,"use");en.href.baseVal="#b";const Pu=re("div","unmute-overlay"),Mn=re("div",void 0,"background"),un=re("div",void 0,"icon"),du=re("svg","unmute-overlay-svg",void 0,{xmlns:Ae,"xmlns:xlink":"http://www.w3.org/1999/xlink",preserveAspectRatio:"xMidYMid",viewBox:"0 0 512 584",width:"100%",height:"100%",scale:"0.8"},Ae),Bn=re("path",void 0,void 0,{fill:"#FFF",stroke:"#FFF",d:"m457.941 256 47.029-47.029c9.372-9.373 9.372-24.568 0-33.941-9.373-9.373-24.568-9.373-33.941 0l-47.029 47.029-47.029-47.029c-9.373-9.373-24.568-9.373-33.941 0-9.372 9.373-9.372 24.568 0 33.941l47.029 47.029-47.029 47.029c-9.372 9.373-9.372 24.568 0 33.941 4.686 4.687 10.827 7.03 16.97 7.03s12.284-2.343 16.971-7.029l47.029-47.03 47.029 47.029c4.687 4.687 10.828 7.03 16.971 7.03s12.284-2.343 16.971-7.029c9.372-9.373 9.372-24.568 0-33.941z"},Ae),Wn=re("path",void 0,void 0,{fill:"#FFF",stroke:"#FFF",d:"m99 160h-55c-24.301 0-44 19.699-44 44v104c0 24.301 19.699 44 44 44h55c2.761 0 5-2.239 5-5v-182c0-2.761-2.239-5-5-5z"},Ae),Ln=re("path",void 0,void 0,{fill:"#FFF",stroke:"#FFF",d:"m280 56h-24c-5.269 0-10.392 1.734-14.578 4.935l-103.459 79.116c-1.237.946-1.963 2.414-1.963 3.972v223.955c0 1.557.726 3.026 1.963 3.972l103.459 79.115c4.186 3.201 9.309 4.936 14.579 4.936h23.999c13.255 0 24-10.745 24-24v-352.001c0-13.255-10.745-24-24-24z"},Ae),Nn=re("text","unmute-text",void 0,{x:"256",y:"560","text-anchor":"middle","font-size":"60px",fill:"#FFF",stroke:"#FFF"},Ae),$n=re("input","virtual-keyboard",void 0,{type:"text",autocapitalize:"off",autocomplete:"off",autocorrect:"off"}),xu=re("div","splash-screen","hidden"),nn=re("svg",void 0,"logo",{xmlns:Ae,"xmlns:xlink":"http://www.w3.org/1999/xlink",preserveAspectRatio:"xMidYMid",viewBox:"0 0 380 150"},Ae),Tu=re("g",void 0,void 0,void 0,Ae),qn=re("path",void 0,void 0,{fill:"#966214",d:"M58.75 85.6q.75-.1 1.5-.35.85-.25 1.65-.75.55-.35 1.05-.8.5-.45.95-1 .5-.5.75-1.2-.05.05-.15.1-.1.15-.25.25l-.1.2q-.15.05-.25.1-.4 0-.8.05-.5-.25-.9-.5-.3-.1-.55-.3l-.6-.6-4.25-6.45-1.5 11.25h3.45m83.15-.2h3.45q.75-.1 1.5-.35.25-.05.45-.15.35-.15.65-.3l.5-.3q.25-.15.5-.35.45-.35.9-.75.45-.35.75-.85l.1-.1q.1-.2.2-.35.2-.3.35-.6l-.3.4-.15.15q-.5.15-1.1.1-.25 0-.4-.05-.5-.15-.8-.4-.15-.1-.25-.25-.3-.3-.55-.6l-.05-.05v-.05l-4.25-6.4-1.5 11.25m-21.15-3.95q-.3-.3-.55-.6l-.05-.05v-.05l-4.25-6.4-1.5 11.25h3.45q.75-.1 1.5-.35.85-.25 1.6-.75.75-.5 1.4-1.1.45-.35.75-.85.35-.5.65-1.05l-.45.55q-.5.15-1.1.1-.9 0-1.45-.7m59.15.3q-.75-.5-1.4-1-3.15-2.55-3.5-6.4l-1.5 11.25h21q-3.1-.25-5.7-.75-5.6-1.05-8.9-3.1m94.2 3.85h3.45q.6-.1 1.2-.3.4-.1.75-.2.35-.15.65-.3.7-.35 1.35-.8.75-.55 1.3-1.25.1-.15.25-.3-2.55-.25-3.25-1.8l-4.2-6.3-1.5 11.25m-45.3-4.85q-.5-.4-.9-.8-2.3-2.35-2.6-5.6l-1.5 11.25h21q-11.25-.95-16-4.85m97.7 4.85q-.3-.05-.6-.05-10.8-1-15.4-4.8-3.15-2.55-3.5-6.35l-1.5 11.2h21Z"},Ae),Un=re("path",void 0,void 0,{fill:"var(--ruffle-orange)",d:"M92.6 54.8q-1.95-1.4-4.5-1.4H60.35q-1.35 0-2.6.45-1.65.55-3.15 1.8-2.75 2.25-3.25 5.25l-1.65 12h.05v.3l5.85 1.15h-9.5q-.5.05-1 .15-.5.15-1 .35-.5.2-.95.45-.5.3-.95.7-.45.35-.85.8-.35.4-.65.85-.3.45-.5.9-.15.45-.3.95l-5.85 41.6H50.3l5-35.5 1.5-11.25 4.25 6.45.6.6q.25.2.55.3.4.25.9.5.4-.05.8-.05.1-.05.25-.1l.1-.2q.15-.1.25-.25.1-.05.15-.1l.3-1.05 1.75-12.3h11.15L75.8 82.6h16.5l2.3-16.25h-.05l.8-5.7q.4-2.45-1-4.2-.35-.4-.75-.8-.25-.25-.55-.5-.2-.2-.45-.35m16.2 18.1h.05l-.05.3 5.85 1.15H105.2q-.5.05-1 .15-.5.15-1 .35-.5.2-.95.45-.5.3-1 .65-.4.4-.8.85-.25.3-.55.65-.05.1-.15.2-.25.45-.4.9-.2.45-.3.95-.1.65-.2 1.25-.2 1.15-.4 2.25l-4.3 30.6q-.25 3 1.75 5.25 1.6 1.8 4 2.15.6.1 1.25.1h27.35q3.25 0 6-2.25.35-.35.7-.55l.3-.2q2-2 2.25-4.5l1.65-11.6q.05-.05.1-.05l1.65-11.35h.05l.7-5.2 1.5-11.25 4.25 6.4v.05l.05.05q.25.3.55.6.1.15.25.25.3.25.8.4.15.05.4.05.6.05 1.1-.1l.15-.15.3-.4.3-1.05 1.3-9.05h-.05l.7-5.05h-.05l.15-1.25h-.05l1.65-11.7h-16.25l-2.65 19.5h.05v.2l-.05.1h.05l5.8 1.15H132.7q-.5.05-1 .15-.5.15-1 .35-.15.05-.3.15-.3.1-.55.25-.05 0-.1.05-.5.3-1 .65-.4.35-.7.7-.55.7-.95 1.45-.35.65-.55 1.4-.15.7-.25 1.4v.05q-.15 1.05-.35 2.05l-1.2 8.75v.1l-2.1 14.7H111.4l2.25-15.55h.05l.7-5.2 1.5-11.25 4.25 6.4v.05l.05.05q.25.3.55.6.55.7 1.45.7.6.05 1.1-.1l.45-.55.3-1.05 1.3-9.05h-.05l.7-5.05h-.05l.15-1.25h-.05l1.65-11.7h-16.25l-2.65 19.5m106.5-41.75q-2.25-2.25-5.5-2.25h-27.75q-3 0-5.75 2.25-1.3.95-2.05 2.1-.45.6-.7 1.2-.2.5-.35 1-.1.45-.15.95l-4.15 29.95h-.05l-.7 5.2h-.05l-.2 1.35h.05l-.05.3 5.85 1.15h-9.45q-2.1.05-3.95 1.6-1.9 1.55-2.25 3.55l-.5 3.5h-.05l-5.3 38.1h16.25l5-35.5 1.5-11.25q.35 3.85 3.5 6.4.65.5 1.4 1 3.3 2.05 8.9 3.1 2.6.5 5.7.75l1.75-11.25h-12.2l.4-2.95h-.05l.7-5.05h-.05q.1-.9.3-1.9.1-.75.2-1.6.85-5.9 2.15-14.9 0-.15.05-.25l.1-.9q.2-1.55.45-3.15h11.25l-3.1 20.8h16.5l4.1-28.05q.15-1.7-.4-3.15-.5-1.1-1.35-2.1m46.65 44.15q-.5.3-1 .65-.4.4-.8.85-.35.4-.7.85-.25.45-.45.9-.15.45-.3.95l-5.85 41.6h16.25l5-35.5 1.5-11.25 4.2 6.3q.7 1.55 3.25 1.8l.05-.1q.25-.4.35-.85l.3-1.05 1.8-14.05v-.05l5.35-37.45h-16.25l-6.15 44.3 5.85 1.15h-9.45q-.5.05-1 .15-.5.15-1 .35-.5.2-.95.45m5.4-38.9q.15-1.7-.4-3.15-.5-1.1-1.35-2.1-2.25-2.25-5.5-2.25h-27.75q-2.3 0-4.45 1.35-.65.35-1.3.9-1.3.95-2.05 2.1-.45.6-.7 1.2-.4.9-.5 1.95l-4.15 29.95h-.05l-.7 5.2h-.05l-.2 1.35h.05l-.05.3 5.85 1.15h-9.45q-2.1.05-3.95 1.6-1.9 1.55-2.25 3.55l-.5 3.5h-.05l-1.2 8.75v.1l-4.1 29.25h16.25l5-35.5 1.5-11.25q.3 3.25 2.6 5.6.4.4.9.8 4.75 3.9 16 4.85l1.75-11.25h-12.2l.4-2.95h-.05l.7-5.05h-.05q.15-.9.3-1.9.1-.75.25-1.6.15-1.25.35-2.65v-.05q.95-6.7 2.35-16.5h11.25l-3.1 20.8h16.5l4.1-28.05M345 66.35h-.05l1.15-8.2q.5-3-1.75-5.25-1.25-1.25-3-1.75-1-.5-2.25-.5h-27.95q-.65 0-1.3.1-2.5.35-4.7 2.15-2.75 2.25-3.25 5.25l-1.95 14.7v.05l-.05.3 5.85 1.15h-9.45q-1.9.05-3.6 1.35-.2.1-.35.25-1.9 1.55-2.25 3.55l-4.85 34.1q-.25 3 1.75 5.25 1.25 1.4 3 1.95 1.05.3 2.25.3H320q3.25 0 6-2.25 2.75-2 3.25-5l2.75-18.5h-16.5l-1.75 11H302.5l2.1-14.75h.05l.85-6 1.5-11.2q.35 3.8 3.5 6.35 4.6 3.8 15.4 4.8.3 0 .6.05h15.75L345 66.35m-16.4-.95-1.25 8.95h-11.3l.4-2.95h-.05l.7-5.05h-.1l.15-.95h11.45Z"},Ae),tn=re("svg",void 0,"loading-animation",{xmlns:Ae,viewBox:"0 0 66 66"},Ae),Zn=re("circle",void 0,"spinner",{fill:"none","stroke-width":"6","stroke-linecap":"round",cx:"33",cy:"33",r:"30"},Ae),an=re("div",void 0,"loadbar"),Hn=re("div",void 0,"loadbar-inner"),rn=re("div","save-manager","modal hidden"),gu=re("div","modal-area","modal-area"),on=re("span",void 0,"close-modal");on.textContent="\xD7";const sn=re("div",void 0,"general-save-options"),Jn=re("span","backup-saves","save-option"),Vn=re("table","local-saves"),dn=re("div","volume-controls-modal","modal hidden"),Mu=re("div",void 0,"modal-area"),cn=re("span",void 0,"close-modal");cn.textContent="\xD7";const cu=re("div","volume-controls"),Kn=re("h2","volume-controls-heading"),Gn=Ze("mute-checkbox-label","mute-checkbox"),Yn=ru("checkbox","mute-checkbox"),wu=re("div",void 0,"slider-container"),Xn=Ze("volume-slider-label","volume-slider"),Qn=ru("range","volume-slider","0","100","1"),et=re("span","volume-slider-text"),ln=re("div","video-modal","modal hidden"),Bu=re("div",void 0,"modal-area"),fn=re("span",void 0,"close-modal");fn.textContent="\xD7";const ut=re("div","video-holder"),hn=re("div","hardware-acceleration-modal","modal hidden"),Wu=re("div",void 0,"modal-area"),mn=re("span",void 0,"close-modal");mn.textContent="\xD7";const lu=document.createElement("a");lu.href="https://github.com/ruffle-rs/ruffle/wiki/Frequently-Asked-Questions-For-Users#chrome-hardware-acceleration",lu.target="_blank",lu.className="acceleration-link",lu.textContent=we("enable-hardware-acceleration");const bn=re("div","context-menu-overlay","hidden"),nt=re("ul","context-menu");ae(He.content,Au),ae(He.content,vu),ae(He.content,Ye),ae(Ye,tu),ae(tu,Qu),ae(Qu,Iu),ae(Iu,Ou),ae(Ou,Fu),ae(Fu,Fn),ae(Fu,Dn),ae(Ou,Du),ae(Du,Pn),ae(Du,Tn),ae(Iu,en),ae(Ye,Pu),ae(Pu,Mn),ae(Pu,un),ae(un,du),ae(du,Bn),ae(du,Wn),ae(du,Ln),ae(du,Nn),ae(Ye,$n),ae(He.content,xu),ae(xu,nn),ae(nn,Tu),ae(Tu,qn),ae(Tu,Un),ae(xu,tn),ae(tn,Zn),ae(xu,an),ae(an,Hn),ae(He.content,rn),ae(rn,gu),ae(gu,on),ae(gu,sn),ae(sn,Jn),ae(gu,Vn),ae(He.content,dn),ae(dn,Mu),ae(Mu,cn),ae(Mu,cu),ae(cu,Kn),ae(cu,Gn),ae(cu,Yn),ae(cu,wu),ae(wu,Xn),ae(wu,Qn),ae(wu,et),ae(He.content,ln),ae(ln,Bu),ae(Bu,fn),ae(Bu,ut),ae(He.content,hn),ae(hn,Wu),ae(Wu,mn),ae(Wu,lu),ae(He.content,bn),ae(bn,nt);const Lu={};function Nu(p,e){const u=Lu[p];if(u!==void 0){if(u.class!==e)throw new Error("Internal naming conflict on "+p);return u.name}let a=0;if(window.customElements!==void 0)for(;a<999;){let s=p;if(a>0&&(s=s+"-"+a),window.customElements.get(s)===void 0)return window.customElements.define(s,e),Lu[p]={class:e,name:s,internalName:p},s;a+=1}throw new Error("Failed to assign custom element "+p)}var fu,$u,ku,qu,hu,pn,iu,Uu,yu;(function(p){p.On="on",p.Off="off",p.Auto="auto"})(fu||(fu={})),(function(p){p.Off="off",p.Fullscreen="fullscreen",p.On="on"})($u||($u={})),(function(p){p.Visible="visible",p.Hidden="hidden"})(ku||(ku={})),(function(p){p.Error="error",p.Warn="warn",p.Info="info",p.Debug="debug",p.Trace="trace"})(qu||(qu={})),(function(p){p.Window="window",p.Opaque="opaque",p.Transparent="transparent",p.Direct="direct",p.Gpu="gpu"})(hu||(hu={})),(function(p){p.WebGpu="webgpu",p.WgpuWebgl="wgpu-webgl",p.Webgl="webgl",p.Canvas="canvas"})(pn||(pn={})),(function(p){p.On="on",p.RightClickOnly="rightClickOnly",p.Off="off"})(iu||(iu={})),(function(p){p.Allow="allow",p.Confirm="confirm",p.Deny="deny"})(Uu||(Uu={})),(function(p){p.All="all",p.Internal="internal",p.None="none"})(yu||(yu={}));const tt={allowScriptAccess:!1,parameters:{},autoplay:fu.Auto,backgroundColor:null,letterbox:$u.Fullscreen,unmuteOverlay:ku.Visible,upgradeToHttps:!0,compatibilityRules:!0,favorFlash:!0,warnOnUnsupportedContent:!0,logLevel:qu.Error,showSwfDownload:!1,contextMenu:iu.On,preloader:!0,splashScreen:!0,maxExecutionDuration:15,base:null,menu:!0,salign:"",forceAlign:!1,quality:"high",scale:"showAll",forceScale:!1,frameRate:null,wmode:hu.Opaque,publicPath:null,polyfills:!0,playerVersion:null,preferredRenderer:null,openUrlMode:Uu.Allow,allowNetworking:yu.All,openInNewTab:null,socketProxy:[],fontSources:[],defaultFonts:{}},vn="application/x-shockwave-flash",xn="application/futuresplash",gn="application/x-shockwave-flash2-preview",wn="application/vnd.adobe.flash.movie";function kn(p,e){const u=(function(a){let s="";try{s=new URL(a,"https://example.com").pathname}catch(g){}if(s&&s.length>=4){const g=s.slice(-4).toLowerCase();if(g===".swf"||g===".spl")return!0}return!1})(p);return e?(function(a,s){switch(a=a.toLowerCase()){case vn.toLowerCase():case xn.toLowerCase():case gn.toLowerCase():case wn.toLowerCase():return!0;default:if(s)switch(a){case"application/octet-stream":case"binary/octet-stream":return!0}}return!1})(e,u):u}const Je={versionNumber:"0.1.0",versionName:"nightly 2023-11-03",versionChannel:"nightly",buildDate:"2023-11-03T00:14:13.838Z",commitHash:"8138b689195dd674f022efa7a8d83b62d34cb9ae"};var at=xe(297),rt=xe.n(at);const mu="https://ruffle.rs",it=/^\s*(\d+(\.\d+)?(%)?)/;let Ru=!1;var Ee,bu;function Zu(p){if(p==null)return{};p instanceof URLSearchParams||(p=new URLSearchParams(p));const e={};for(const[u,a]of p)e[u]=a.toString();return e}(function(p){p[p.Unknown=0]="Unknown",p[p.CSPConflict=1]="CSPConflict",p[p.FileProtocol=2]="FileProtocol",p[p.InvalidWasm=3]="InvalidWasm",p[p.JavascriptConfiguration=4]="JavascriptConfiguration",p[p.JavascriptConflict=5]="JavascriptConflict",p[p.WasmCors=6]="WasmCors",p[p.WasmDownload=7]="WasmDownload",p[p.WasmMimeType=8]="WasmMimeType",p[p.WasmNotFound=9]="WasmNotFound",p[p.WasmDisabledMicrosoftEdge=10]="WasmDisabledMicrosoftEdge",p[p.SwfFetchError=11]="SwfFetchError",p[p.SwfCors=12]="SwfCors"})(Ee||(Ee={}));class yn{constructor(e,u){this.x=e,this.y=u}distanceTo(e){const u=e.x-this.x,a=e.y-this.y;return Math.sqrt(u*u+a*a)}}class Ie{constructor(e="#",u=we("view-error-details")){this.url=e,this.label=u}}class Xe extends HTMLElement{get readyState(){return this._readyState}get metadata(){return this._metadata}constructor(){super(),this.contextMenuForceDisabled=!1,this.isTouch=!1,this.contextMenuSupported=!1,this.panicked=!1,this.rendererDebugInfo="",this.longPressTimer=null,this.pointerDownPosition=null,this.pointerMoveMaxDistance=0,this.config={},this.shadow=this.attachShadow({mode:"open"}),this.shadow.appendChild(He.content.cloneNode(!0)),this.dynamicStyles=this.shadow.getElementById("dynamic-styles"),this.staticStyles=this.shadow.getElementById("static-styles"),this.container=this.shadow.getElementById("container"),this.playButton=this.shadow.getElementById("play-button"),this.playButton.addEventListener("click",(()=>this.play())),this.unmuteOverlay=this.shadow.getElementById("unmute-overlay"),this.splashScreen=this.shadow.getElementById("splash-screen"),this.virtualKeyboard=this.shadow.getElementById("virtual-keyboard"),this.virtualKeyboard.addEventListener("input",this.virtualKeyboardInput.bind(this)),this.saveManager=this.shadow.getElementById("save-manager"),this.videoModal=this.shadow.getElementById("video-modal"),this.hardwareAccelerationModal=this.shadow.getElementById("hardware-acceleration-modal"),this.volumeControls=this.shadow.getElementById("volume-controls-modal"),this.addModalJavaScript(this.saveManager),this.addModalJavaScript(this.volumeControls),this.addModalJavaScript(this.videoModal),this.addModalJavaScript(this.hardwareAccelerationModal),this.volumeSettings=new ot(!1,100),this.addVolumeControlsJavaScript(this.volumeControls);const e=this.saveManager.querySelector("#backup-saves");e&&(e.addEventListener("click",this.backupSaves.bind(this)),e.innerText=we("save-backup-all"));const u=this.unmuteOverlay.querySelector("#unmute-overlay-svg");u&&(u.querySelector("#unmute-text").textContent=we("click-to-unmute")),this.contextMenuOverlay=this.shadow.getElementById("context-menu-overlay"),this.contextMenuElement=this.shadow.getElementById("context-menu"),document.documentElement.addEventListener("pointerdown",this.checkIfTouch.bind(this)),this.addEventListener("contextmenu",this.showContextMenu.bind(this)),this.container.addEventListener("pointerdown",this.pointerDown.bind(this)),this.container.addEventListener("pointermove",this.checkLongPressMovement.bind(this)),this.container.addEventListener("pointerup",this.checkLongPress.bind(this)),this.container.addEventListener("pointercancel",this.clearLongPressTimer.bind(this)),this.addEventListener("fullscreenchange",this.fullScreenChange.bind(this)),this.addEventListener("webkitfullscreenchange",this.fullScreenChange.bind(this)),this.instance=null,this.onFSCommand=null,this._readyState=bu.HaveNothing,this._metadata=null,this.lastActivePlayingState=!1,this.setupPauseOnTabHidden()}addModalJavaScript(e){const u=e.querySelector("#video-holder");this.container.addEventListener("click",(()=>{e.classList.add("hidden"),u&&(u.textContent="")}));const a=e.querySelector(".modal-area");a&&a.addEventListener("click",(g=>g.stopPropagation()));const s=e.querySelector(".close-modal");s&&s.addEventListener("click",(()=>{e.classList.add("hidden"),u&&(u.textContent="")}))}addVolumeControlsJavaScript(e){const u=e.querySelector("#mute-checkbox"),a=e.querySelector("#volume-slider"),s=e.querySelector("#volume-slider-text"),g=e.querySelector("#volume-controls-heading"),L=e.querySelector("#mute-checkbox-label"),K=e.querySelector("#volume-slider-label");g.textContent=we("volume-controls"),L.textContent=we("volume-controls-mute"),K.textContent=we("volume-controls-volume"),u.checked=this.volumeSettings.isMuted,a.disabled=u.checked,a.valueAsNumber=this.volumeSettings.volume,K.style.color=u.checked?"grey":"black",s.style.color=u.checked?"grey":"black",s.textContent=String(this.volumeSettings.volume),u.addEventListener("change",(()=>{var ne;a.disabled=u.checked,K.style.color=u.checked?"grey":"black",s.style.color=u.checked?"grey":"black",this.volumeSettings.isMuted=u.checked,(ne=this.instance)===null||ne===void 0||ne.set_volume(this.volumeSettings.get_volume())})),a.addEventListener("input",(()=>{var ne;s.textContent=a.value,this.volumeSettings.volume=a.valueAsNumber,(ne=this.instance)===null||ne===void 0||ne.set_volume(this.volumeSettings.get_volume())}))}setupPauseOnTabHidden(){document.addEventListener("visibilitychange",(()=>{this.instance&&(document.hidden&&(this.lastActivePlayingState=this.instance.is_playing(),this.instance.pause()),document.hidden||this.lastActivePlayingState!==!0||this.instance.play())}),!1)}get height(){return this.getAttribute("height")||""}set height(e){this.setAttribute("height",e)}get width(){return this.getAttribute("width")||""}set width(e){this.setAttribute("width",e)}get type(){return this.getAttribute("type")||""}set type(e){this.setAttribute("type",e)}connectedCallback(){this.updateStyles(),(function(e){if(!e.sheet)return;const u=[`:host {
            all: initial;
            pointer-events: inherit;

            --ruffle-blue: #37528c;
            --ruffle-orange: #ffad33;

            display: inline-block;
            position: relative;
            /* Default width/height; this will get overridden by user styles/attributes. */
            width: 550px;
            height: 400px;
            font-family: Arial, sans-serif;
            letter-spacing: 0.4px;
            touch-action: none;
            user-select: none;
            -webkit-user-select: none;
            -webkit-tap-highlight-color: transparent;
        }`,`:host(:-webkit-full-screen) {
            display: block;
            width: 100% !important;
            height: 100% !important;
        }`,`.hidden {
            display: none !important;
        }`,`#container,
        #play-button,
        #unmute-overlay,
        #unmute-overlay .background,
        #panic,
        #splash-screen,
        #message-overlay {
            position: absolute;
            top: 0;
            bottom: 0;
            left: 0;
            right: 0;
        }`,`#container {
            overflow: hidden;
        }`,`#container canvas {
            width: 100%;
            height: 100%;
        }`,`#play-button,
        #unmute-overlay {
            cursor: pointer;
            display: none;
        }`,`#unmute-overlay .background {
            background: black;
            opacity: 0.7;
        }`,`#play-button .icon,
        #unmute-overlay .icon {
            position: absolute;
            top: 50%;
            left: 50%;
            width: 50%;
            height: 50%;
            max-width: 384px;
            max-height: 384px;
            transform: translate(-50%, -50%);
            opacity: 0.8;
        }`,`#play-button:hover .icon,
        #unmute-overlay:hover .icon {
            opacity: 1;
        }`,`#panic {
            font-size: 20px;
            text-align: center;
            background: linear-gradient(180deg, #fd3a40 0%, #fda138 100%);
            color: white;
            display: flex;
            flex-flow: column;
            justify-content: space-around;
        }`,`#panic a {
            color: var(--ruffle-blue);
            font-weight: bold;
        }`,`#panic-title {
            font-size: xxx-large;
            font-weight: bold;
        }`,`#panic-body.details {
            flex: 0.9;
            margin: 0 10px;
        }`,`#panic-body textarea {
            width: 100%;
            height: 100%;
            resize: none;
        }`,`#panic ul {
            padding: 0;
            display: flex;
            list-style-type: none;
            justify-content: space-evenly;
        }`,`#message-overlay {
            position: absolute;
            background: var(--ruffle-blue);
            color: var(--ruffle-orange);
            opacity: 1;
            z-index: 2;
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: auto;
        }`,`#message-overlay .message {
            text-align: center;
            max-height: 100%;
            max-width: 100%;
            padding: 5%;
            font-size: 20px;
        }`,`#message-overlay p {
            margin: 0.5em 0;
        }`,`#message-overlay .message div {
            display: flex;
            justify-content: center;
            flex-wrap: wrap;
            column-gap: 1em;
        }`,`#message-overlay a, #message-overlay button {
            cursor: pointer;
            background: var(--ruffle-blue);
            color: var(--ruffle-orange);
            border: 2px solid var(--ruffle-orange);
            font-weight: bold;
            font-size: 1.25em;
            border-radius: 0.6em;
            padding: 10px;
            text-decoration: none;
            margin: 2% 0;
        }`,`#message-overlay a:hover, #message-overlay button:hover {
            background: #ffffff4c;
        }`,`#continue-btn {
             cursor: pointer;
             background: var(--ruffle-blue);
             color: var(--ruffle-orange);
             border: 2px solid var(--ruffle-orange);
             font-weight: bold;
             font-size: 20px;
             border-radius: 20px;
             padding: 10px;
        }`,`#continue-btn:hover {
            background: #ffffff4c;
        }`,`#context-menu-overlay {
            width: 100%;
            height: 100%;
            z-index: 1;
            position: absolute;
        }`,`#context-menu {
            color: black;
            background: #fafafa;
            border: 1px solid gray;
            box-shadow: 0px 5px 10px -5px black;
            position: absolute;
            font-size: 14px;
            text-align: left;
            list-style: none;
            padding: 0;
            margin: 0;
        }`,`#context-menu .menu-item {
            padding: 5px 10px;
            cursor: pointer;
            color: black;
        }`,`#context-menu .menu-item.disabled {
            cursor: default;
            color: gray;
        }`,`#context-menu .menu-item:not(.disabled):hover {
            background: lightgray;
        }`,`#context-menu .menu-separator hr {
            border: none;
            border-bottom: 1px solid lightgray;
            margin: 2px;
        }`,`#splash-screen {
            display: flex;
            flex-direction: column;
            background: var(--splash-screen-background, var(--preloader-background, var(--ruffle-blue)));
            align-items: center;
            justify-content: center;
        }`,`.loadbar {
            width: 100%;
            max-width: 316px;
            max-height: 10px;
            height: 20%;
            background: #253559;
        }`,`.loadbar-inner {
            width: 0px;
            max-width: 100%;
            height: 100%;
            background: var(--ruffle-orange);
        }`,`.logo {
            display: var(--logo-display, block);
            max-width: 380px;
            max-height: 150px;
        }`,`.loading-animation {
            max-width: 28px;
            max-height: 28px;
            margin-bottom: 2%;
            width: 10%;
            aspect-ratio: 1;
        }`,`.spinner {
            stroke-dasharray: 180;
            stroke-dashoffset: 135;
            stroke: var(--ruffle-orange);
            transform-origin: 50% 50%;
            animation: rotate 1.5s linear infinite;
        }`,`@keyframes rotate {
            to {
                transform: rotate(360deg);
            }
        }`,`#virtual-keyboard {
            position: absolute;
            opacity: 0;
            top: -100px;
            width: 1px;
            height: 1px;
        }`,`.modal {
            height: inherit;
            user-select: text;
        }`,`.modal-area {
            position: sticky;
            background: white;
            width: fit-content;
            padding: 16px 28px 16px 16px;
            border: 3px solid black;
            margin: auto;
        }`,`#modal-area {
            height: 500px;
            max-height: calc(100% - 38px);
            min-height: 80px;
        }`,`#restore-save {
            display: none;
        }`,`.replace-save {
            display: none;
        }`,`.save-option {
            display: inline-block;
            padding: 3px 10px;
            margin: 5px 2px;
            cursor: pointer;
            border-radius: 50px;
            background-color: var(--ruffle-blue);
            color: white;
        }`,`.close-modal {
            position: absolute;
            top: 5px;
            right: 10px;
            cursor: pointer;
            font-size: x-large;
        }`,`.general-save-options {
            text-align: center;
            padding-bottom: 8px;
            border-bottom: 2px solid #888;
        }`,`#local-saves {
            border-collapse: collapse;
            overflow-y: auto;
            display: block;
            padding-right: 16px;
            height: calc(100% - 45px);
            min-height: 30px;
        }`,`#local-saves td {
            border-bottom: 1px solid #bbb;
            height: 30px;
        }`,`#local-saves tr td:nth-child(1) {
            padding-right: 1em;
            word-break: break-all;
        }`,`#local-saves tr:nth-child(even) {
            background-color: #f2f2f2;
        }`,`#video-holder {
            padding-top: 20px;
        }`,`.slider-container {
            margin-top: 10px;
            display: flex;
            align-items: center;
        }`,`#volume-slider {
            margin-left: 10px;
            margin-right: 10px;
        }`,`#volume-slider-text {
            text-align: right;
            width: 28px;
        }`,`.acceleration-link {
            color: var(--ruffle-blue);
            text-decoration: none;
        }`,`.acceleration-link:hover {
            text-decoration: underline;
        }`];(function(a,s){for(const g of s)try{a.insertRule(g)}catch(L){}})(e.sheet,u)})(this.staticStyles)}static get observedAttributes(){return["width","height"]}attributeChangedCallback(e,u,a){e!=="width"&&e!=="height"||this.updateStyles()}disconnectedCallback(){this.destroy()}updateStyles(){if(this.dynamicStyles.sheet){if(this.dynamicStyles.sheet.cssRules)for(let a=this.dynamicStyles.sheet.cssRules.length-1;a>=0;a--)this.dynamicStyles.sheet.deleteRule(a);const e=this.attributes.getNamedItem("width");if(e!=null){const a=Xe.htmlDimensionToCssDimension(e.value);a!==null&&this.dynamicStyles.sheet.insertRule(`:host { width: ${a}; }`)}const u=this.attributes.getNamedItem("height");if(u!=null){const a=Xe.htmlDimensionToCssDimension(u.value);a!==null&&this.dynamicStyles.sheet.insertRule(`:host { height: ${a}; }`)}}}isUnusedFallbackObject(){const e=(function(u){const a=Lu[u];return a!==void 0?{internalName:u,name:a.name,class:a.class}:null})("ruffle-object");if(e!==null){let u=this.parentNode;for(;u!==document&&u!==null;){if(u.nodeName===e.name)return!0;u=u.parentNode}}return!1}async ensureFreshInstance(){var e,u,a,s,g,L,K,ne,ie,oe,me;if(this.destroy(),this.loadedConfig&&this.loadedConfig.splashScreen!==!1&&this.loadedConfig.preloader!==!1&&this.showSplashScreen(),this.loadedConfig&&this.loadedConfig.preloader===!1&&console.warn("The configuration option preloader has been replaced with splashScreen. If you own this website, please update the configuration."),this.loadedConfig&&this.loadedConfig.maxExecutionDuration&&typeof this.loadedConfig.maxExecutionDuration!="number"&&console.warn("Configuration: An obsolete format for duration for 'maxExecutionDuration' was used, please use a single number indicating seconds instead. For instance '15' instead of '{secs: 15, nanos: 0}'."),this.loadedConfig&&typeof this.loadedConfig.contextMenu=="boolean"&&console.warn('The configuration option contextMenu no longer takes a boolean. Use "on", "off", or "rightClickOnly".'),this.instance=await _(this.container,this,this.loadedConfig||{},this.onRuffleDownloadProgress.bind(this)).catch((de=>{if(console.error(`Serious error loading Ruffle: ${de}`),window.location.protocol==="file:")de.ruffleIndexError=Ee.FileProtocol;else{de.ruffleIndexError=Ee.WasmNotFound;const Se=String(de.message).toLowerCase();Se.includes("mime")?de.ruffleIndexError=Ee.WasmMimeType:Se.includes("networkerror")||Se.includes("failed to fetch")?de.ruffleIndexError=Ee.WasmCors:Se.includes("disallowed by embedder")?de.ruffleIndexError=Ee.CSPConflict:de.name==="CompileError"?de.ruffleIndexError=Ee.InvalidWasm:Se.includes("could not download wasm module")&&de.name==="TypeError"?de.ruffleIndexError=Ee.WasmDownload:de.name==="TypeError"?de.ruffleIndexError=Ee.JavascriptConflict:navigator.userAgent.includes("Edg")&&Se.includes("webassembly is not defined")&&(de.ruffleIndexError=Ee.WasmDisabledMicrosoftEdge)}throw this.panic(de),de})),(e=this.loadedConfig)===null||e===void 0?void 0:e.fontSources)for(const de of this.loadedConfig.fontSources)try{const Se=await fetch(de);this.instance.add_font(de,new Uint8Array(await Se.arrayBuffer()))}catch(Se){console.warn(`Couldn't download font source from ${de}`,Se)}!((a=(u=this.loadedConfig)===null||u===void 0?void 0:u.defaultFonts)===null||a===void 0)&&a.sans&&this.instance.set_default_font("sans",(s=this.loadedConfig)===null||s===void 0?void 0:s.defaultFonts.sans),!((L=(g=this.loadedConfig)===null||g===void 0?void 0:g.defaultFonts)===null||L===void 0)&&L.serif&&this.instance.set_default_font("serif",(K=this.loadedConfig)===null||K===void 0?void 0:K.defaultFonts.serif),!((ie=(ne=this.loadedConfig)===null||ne===void 0?void 0:ne.defaultFonts)===null||ie===void 0)&&ie.typewriter&&this.instance.set_default_font("typewriter",(oe=this.loadedConfig)===null||oe===void 0?void 0:oe.defaultFonts.typewriter),this.instance.set_volume(this.volumeSettings.get_volume()),this.rendererDebugInfo=this.instance.renderer_debug_info(),this.rendererDebugInfo.includes("Adapter Device Type: Cpu")&&this.container.addEventListener("mouseover",this.openHardwareAccelerationModal.bind(this),{once:!0});const he=this.instance.renderer_name(),fe=this.instance.constructor;if(console.log("%cNew Ruffle instance created (Version: "+Je.versionName+" | WebAssembly extensions: "+(fe.is_wasm_simd_used()?"ON":"OFF")+" | Used renderer: "+(he!=null?he:"")+")","background: #37528C; color: #FFAD33"),this.audioState()!=="running"&&(this.container.style.visibility="hidden",await new Promise((de=>{window.setTimeout((()=>{de()}),200)})),this.container.style.visibility=""),this.unmuteAudioContext(),navigator.userAgent.toLowerCase().includes("android")&&this.container.addEventListener("click",(()=>this.virtualKeyboard.blur())),!this.loadedConfig||this.loadedConfig.autoplay===fu.On||this.loadedConfig.autoplay!==fu.Off&&this.audioState()==="running"){if(this.play(),this.audioState()!=="running"){this.loadedConfig&&this.loadedConfig.unmuteOverlay===ku.Hidden||(this.unmuteOverlay.style.display="block"),this.container.addEventListener("click",this.unmuteOverlayClicked.bind(this),{once:!0});const de=(me=this.instance)===null||me===void 0?void 0:me.audio_context();de&&(de.onstatechange=()=>{de.state==="running"&&this.unmuteOverlayClicked(),de.onstatechange=null})}}else this.playButton.style.display="block"}onRuffleDownloadProgress(e,u){const a=this.splashScreen.querySelector(".loadbar-inner"),s=this.splashScreen.querySelector(".loadbar");Number.isNaN(u)?s&&(s.style.display="none"):a.style.width=e/u*100+"%"}destroy(){this.instance&&(this.instance.destroy(),this.instance=null,this._metadata=null,this._readyState=bu.HaveNothing,console.log("Ruffle instance destroyed."))}checkOptions(e){if(typeof e=="string")return{url:e};const u=(a,s)=>{if(!a){const g=new TypeError(s);throw g.ruffleIndexError=Ee.JavascriptConfiguration,this.panic(g),g}};return u(e!==null&&typeof e=="object","Argument 0 must be a string or object"),u("url"in e||"data"in e,"Argument 0 must contain a `url` or `data` key"),u(!("url"in e)||typeof e.url=="string","`url` must be a string"),e}async reload(){if(!this.loadedConfig)throw new Error("Cannot reload if load wasn't first called");await this.load(this.loadedConfig)}async load(e){var u,a;if(e=this.checkOptions(e),this.isConnected&&!this.isUnusedFallbackObject()){if(!Vu(this))try{this.loadedConfig=Object.assign(Object.assign(Object.assign(Object.assign({},tt),(a=(u=window.RufflePlayer)===null||u===void 0?void 0:u.config)!==null&&a!==void 0?a:{}),this.config),e),this.loadedConfig.backgroundColor&&this.loadedConfig.wmode!==hu.Transparent&&(this.container.style.backgroundColor=this.loadedConfig.backgroundColor),await this.ensureFreshInstance(),"url"in e?(console.log(`Loading SWF file ${e.url}`),this.swfUrl=new URL(e.url,document.baseURI),this.instance.stream_from(this.swfUrl.href,Zu(e.parameters))):"data"in e&&(console.log("Loading SWF data"),this.instance.load_data(new Uint8Array(e.data),Zu(e.parameters),e.swfFileName||"movie.swf"))}catch(s){console.error(`Serious error occurred loading SWF file: ${s}`);const g=new Error(s);throw g.message.includes("Error parsing config")&&(g.ruffleIndexError=Ee.JavascriptConfiguration),this.panic(g),g}}else console.warn("Ignoring attempt to play a disconnected or suspended Ruffle element")}play(){this.instance&&(this.instance.play(),this.playButton.style.display="none")}get isPlaying(){return!!this.instance&&this.instance.is_playing()}get volume(){return this.instance?this.instance.volume():1}set volume(e){this.instance&&this.instance.set_volume(e)}get fullscreenEnabled(){return!(!document.fullscreenEnabled&&!document.webkitFullscreenEnabled)}get isFullscreen(){return(document.fullscreenElement||document.webkitFullscreenElement)===this}setFullscreen(e){this.fullscreenEnabled&&e!==this.isFullscreen&&(e?this.enterFullscreen():this.exitFullscreen())}enterFullscreen(){const e={navigationUI:"hide"};this.requestFullscreen?this.requestFullscreen(e):this.webkitRequestFullscreen?this.webkitRequestFullscreen(e):this.webkitRequestFullScreen&&this.webkitRequestFullScreen(e)}exitFullscreen(){document.exitFullscreen?document.exitFullscreen():document.webkitExitFullscreen?document.webkitExitFullscreen():document.webkitCancelFullScreen&&document.webkitCancelFullScreen()}fullScreenChange(){var e;(e=this.instance)===null||e===void 0||e.set_fullscreen(this.isFullscreen)}saveFile(e,u){const a=URL.createObjectURL(e),s=document.createElement("a");s.href=a,s.style.display="none",s.download=u,document.body.appendChild(s),s.click(),document.body.removeChild(s),URL.revokeObjectURL(a)}checkIfTouch(e){this.isTouch=e.pointerType==="touch"||e.pointerType==="pen"}base64ToBlob(e,u){const a=atob(e),s=new ArrayBuffer(a.length),g=new Uint8Array(s);for(let L=0;L<a.length;L++)g[L]=a.charCodeAt(L);return new Blob([s],{type:u})}isB64SOL(e){try{return atob(e).slice(6,10)==="TCSO"}catch(u){return!1}}confirmReloadSave(e,u,a){if(this.isB64SOL(u)&&localStorage[e]){if(!a&&!confirm(we("save-delete-prompt")))return;const s=this.swfUrl?this.swfUrl.pathname:"",g=this.swfUrl?this.swfUrl.hostname:document.location.hostname,L=e.split("/").slice(1,-1).join("/");if(s.includes(L)&&e.startsWith(g))return void(confirm(we("save-reload-prompt",{action:a?"replace":"delete"}))&&this.loadedConfig&&(this.destroy(),a?localStorage.setItem(e,u):localStorage.removeItem(e),this.reload(),this.populateSaves(),this.saveManager.classList.add("hidden")));a?localStorage.setItem(e,u):localStorage.removeItem(e),this.populateSaves(),this.saveManager.classList.add("hidden")}}replaceSOL(e,u){const a=e.target,s=new FileReader;s.addEventListener("load",(()=>{if(s.result&&typeof s.result=="string"){const g=new RegExp("data:.*;base64,"),L=s.result.replace(g,"");this.confirmReloadSave(u,L,!0)}})),a&&a.files&&a.files.length>0&&a.files[0]&&s.readAsDataURL(a.files[0])}deleteSave(e){const u=localStorage.getItem(e);u&&this.confirmReloadSave(e,u,!1)}populateSaves(){const e=this.saveManager.querySelector("#local-saves");if(e){try{if(localStorage===null)return}catch(u){return}e.textContent="",Object.keys(localStorage).forEach((u=>{const a=u.split("/").pop(),s=localStorage.getItem(u);if(a&&s&&this.isB64SOL(s)){const g=document.createElement("TR"),L=document.createElement("TD");L.textContent=a,L.title=u;const K=document.createElement("TD"),ne=document.createElement("SPAN");ne.textContent=we("save-download"),ne.className="save-option",ne.addEventListener("click",(()=>{const de=this.base64ToBlob(s,"application/octet-stream");this.saveFile(de,a+".sol")})),K.appendChild(ne);const ie=document.createElement("TD"),oe=document.createElement("INPUT");oe.type="file",oe.accept=".sol",oe.className="replace-save",oe.id="replace-save-"+u;const me=document.createElement("LABEL");me.htmlFor="replace-save-"+u,me.textContent=we("save-replace"),me.className="save-option",oe.addEventListener("change",(de=>this.replaceSOL(de,u))),ie.appendChild(oe),ie.appendChild(me);const he=document.createElement("TD"),fe=document.createElement("SPAN");fe.textContent=we("save-delete"),fe.className="save-option",fe.addEventListener("click",(()=>this.deleteSave(u))),he.appendChild(fe),g.appendChild(L),g.appendChild(K),g.appendChild(ie),g.appendChild(he),e.appendChild(g)}}))}}async backupSaves(){const e=new(rt()),u=[];Object.keys(localStorage).forEach((s=>{let g=String(s.split("/").pop());const L=localStorage.getItem(s);if(L&&this.isB64SOL(L)){const K=this.base64ToBlob(L,"application/octet-stream"),ne=u.filter((ie=>ie===g)).length;u.push(g),ne>0&&(g+=` (${ne+1})`),e.file(g+".sol",K)}}));const a=await e.generateAsync({type:"blob"});this.saveFile(a,"saves.zip")}openHardwareAccelerationModal(){this.hardwareAccelerationModal.classList.remove("hidden")}openSaveManager(){this.saveManager.classList.remove("hidden")}openVolumeControls(){this.volumeControls.classList.remove("hidden")}async downloadSwf(){try{if(this.swfUrl){console.log("Downloading SWF: "+this.swfUrl);const e=await fetch(this.swfUrl.href);if(!e.ok)return void console.error("SWF download failed");const u=await e.blob();this.saveFile(u,(function(a){const s=a.pathname;return s.substring(s.lastIndexOf("/")+1)})(this.swfUrl))}else console.error("SWF download failed")}catch(e){console.error("SWF download failed")}}virtualKeyboardInput(){const e=this.virtualKeyboard,u=e.value;for(const a of u)for(const s of["keydown","keyup"])this.dispatchEvent(new KeyboardEvent(s,{key:a,bubbles:!0}));e.value=""}openVirtualKeyboard(){navigator.userAgent.toLowerCase().includes("android")?setTimeout((()=>{this.virtualKeyboard.focus({preventScroll:!0})}),100):this.virtualKeyboard.focus({preventScroll:!0})}isVirtualKeyboardFocused(){return this.shadow.activeElement===this.virtualKeyboard}contextMenuItems(){const u=[],a=()=>{u.length>0&&u[u.length-1]!==null&&u.push(null)};this.instance&&this.isPlaying&&(this.instance.prepare_context_menu().forEach(((g,L)=>{g.separatorBefore&&a(),u.push({text:g.caption+(g.checked?" (\u2713)":""),onClick:()=>{var K;return(K=this.instance)===null||K===void 0?void 0:K.run_context_menu_callback(L)},enabled:g.enabled})})),a()),this.fullscreenEnabled&&(this.isFullscreen?u.push({text:we("context-menu-exit-fullscreen"),onClick:()=>{var g;return(g=this.instance)===null||g===void 0?void 0:g.set_fullscreen(!1)}}):u.push({text:we("context-menu-enter-fullscreen"),onClick:()=>{var g;return(g=this.instance)===null||g===void 0?void 0:g.set_fullscreen(!0)}})),u.push({text:we("context-menu-volume-controls"),onClick:()=>{this.openVolumeControls()}}),this.instance&&this.swfUrl&&this.loadedConfig&&this.loadedConfig.showSwfDownload===!0&&(a(),u.push({text:we("context-menu-download-swf"),onClick:this.downloadSwf.bind(this)})),navigator.clipboard&&window.isSecureContext&&u.push({text:we("context-menu-copy-debug-info"),onClick:()=>navigator.clipboard.writeText(this.getPanicData())}),this.populateSaves();const s=this.saveManager.querySelector("#local-saves");return s&&s.textContent!==""&&u.push({text:we("context-menu-open-save-manager"),onClick:this.openSaveManager.bind(this)}),a(),u.push({text:we("context-menu-about-ruffle",{flavor:w?"extension":"",version:Je.versionName}),onClick(){window.open(mu,"_blank")}}),this.isTouch&&(a(),u.push({text:we("context-menu-hide"),onClick:()=>this.contextMenuForceDisabled=!0})),u}pointerDown(e){this.pointerDownPosition=new yn(e.pageX,e.pageY),this.pointerMoveMaxDistance=0,this.startLongPressTimer()}clearLongPressTimer(){this.longPressTimer&&(clearTimeout(this.longPressTimer),this.longPressTimer=null)}startLongPressTimer(){this.clearLongPressTimer(),this.longPressTimer=setTimeout((()=>this.clearLongPressTimer()),800)}checkLongPressMovement(e){if(this.pointerDownPosition!==null){const u=new yn(e.pageX,e.pageY),a=this.pointerDownPosition.distanceTo(u);a>this.pointerMoveMaxDistance&&(this.pointerMoveMaxDistance=a)}}checkLongPress(e){this.longPressTimer?this.clearLongPressTimer():!this.contextMenuSupported&&e.pointerType!=="mouse"&&this.pointerMoveMaxDistance<15&&this.showContextMenu(e)}showContextMenu(e){var u,a,s;const g=Array.from(this.shadow.querySelectorAll(".modal")).some((me=>!me.classList.contains("hidden")));if(this.panicked||g||(e.preventDefault(),e.type==="contextmenu"?(this.contextMenuSupported=!0,document.documentElement.addEventListener("click",this.hideContextMenu.bind(this),{once:!0})):(document.documentElement.addEventListener("pointerup",this.hideContextMenu.bind(this),{once:!0}),e.stopPropagation()),[!1,iu.Off].includes((a=(u=this.loadedConfig)===null||u===void 0?void 0:u.contextMenu)!==null&&a!==void 0?a:iu.On)||this.isTouch&&((s=this.loadedConfig)===null||s===void 0?void 0:s.contextMenu)===iu.RightClickOnly||this.contextMenuForceDisabled))return;for(;this.contextMenuElement.firstChild;)this.contextMenuElement.removeChild(this.contextMenuElement.firstChild);for(const me of this.contextMenuItems())if(me===null){const he=document.createElement("li");he.className="menu-separator";const fe=document.createElement("hr");he.appendChild(fe),this.contextMenuElement.appendChild(he)}else{const{text:he,onClick:fe,enabled:de}=me,Se=document.createElement("li");Se.className="menu-item",Se.textContent=he,this.contextMenuElement.appendChild(Se),de!==!1?Se.addEventListener(this.contextMenuSupported?"click":"pointerup",fe):Se.classList.add("disabled")}this.contextMenuElement.style.left="0",this.contextMenuElement.style.top="0",this.contextMenuOverlay.classList.remove("hidden");const L=this.getBoundingClientRect(),K=e.clientX-L.x,ne=e.clientY-L.y,ie=L.width-this.contextMenuElement.clientWidth-1,oe=L.height-this.contextMenuElement.clientHeight-1;this.contextMenuElement.style.left=Math.floor(Math.min(K,ie))+"px",this.contextMenuElement.style.top=Math.floor(Math.min(ne,oe))+"px"}hideContextMenu(){var e;(e=this.instance)===null||e===void 0||e.clear_custom_menu_items(),this.contextMenuOverlay.classList.add("hidden")}pause(){this.instance&&(this.instance.pause(),this.playButton.style.display="block")}audioState(){if(this.instance){const e=this.instance.audio_context();return e&&e.state||"running"}return"suspended"}unmuteOverlayClicked(){if(this.instance){if(this.audioState()!=="running"){const e=this.instance.audio_context();e&&e.resume()}this.unmuteOverlay.style.display="none"}}unmuteAudioContext(){Ru||(navigator.maxTouchPoints<1?Ru=!0:this.container.addEventListener("click",(()=>{var e;if(Ru)return;const u=(e=this.instance)===null||e===void 0?void 0:e.audio_context();if(!u)return;const a=new Audio;a.src=(()=>{const s=new ArrayBuffer(10),g=new DataView(s),L=u.sampleRate;return g.setUint32(0,L,!0),g.setUint32(4,L,!0),g.setUint16(8,1,!0),`data:audio/wav;base64,UklGRisAAABXQVZFZm10IBAAAAABAAEA${window.btoa(String.fromCharCode(...new Uint8Array(s))).slice(0,13)}AgAZGF0YQcAAACAgICAgICAAAA=`})(),a.load(),a.play().then((()=>{Ru=!0})).catch((s=>{console.warn(`Failed to play dummy sound: ${s}`)}))}),{once:!0}))}copyElement(e){if(e){for(const u of e.attributes)if(u.specified){if(u.name==="title"&&u.value==="Adobe Flash Player")continue;try{this.setAttribute(u.name,u.value)}catch(a){console.warn(`Unable to set attribute ${u.name} on Ruffle instance`)}}for(const u of Array.from(e.children))this.appendChild(u)}}static htmlDimensionToCssDimension(e){if(e){const u=e.match(it);if(u){let a=u[1];return u[3]||(a+="px"),a}}return null}onCallbackAvailable(e){const u=this.instance;this[e]=(...a)=>u==null?void 0:u.call_exposed_callback(e,a)}set traceObserver(e){var u;(u=this.instance)===null||u===void 0||u.set_trace_observer(e)}getPanicData(){let e=`
# Player Info
`;if(e+=`Allows script access: ${!!this.loadedConfig&&this.loadedConfig.allowScriptAccess}
`,e+=`${this.rendererDebugInfo}
`,e+=this.debugPlayerInfo(),e+=`
# Page Info
`,e+=`Page URL: ${document.location.href}
`,this.swfUrl&&(e+=`SWF URL: ${this.swfUrl}
`),e+=`
# Browser Info
`,e+=`User Agent: ${window.navigator.userAgent}
`,e+=`Platform: ${window.navigator.platform}
`,e+=`Has touch support: ${window.navigator.maxTouchPoints>0}
`,e+=`
# Ruffle Info
`,e+=`Version: ${Je.versionNumber}
`,e+=`Name: ${Je.versionName}
`,e+=`Channel: ${Je.versionChannel}
`,e+=`Built: ${Je.buildDate}
`,e+=`Commit: ${Je.commitHash}
`,e+=`Is extension: ${w}
`,e+=`
# Metadata
`,this.metadata)for(const[u,a]of Object.entries(this.metadata))e+=`${u}: ${a}
`;return e}createErrorFooter(e){const u=document.createElement("ul");for(const a of e){const s=document.createElement("li"),g=document.createElement("a");g.href=a.url,g.textContent=a.label,a.url==="#"?g.id="panic-view-details":g.target="_top",s.appendChild(g),u.appendChild(s)}return u}panic(e){var u;if(this.panicked||(this.panicked=!0,this.hideSplashScreen(),e instanceof Error&&(e.name==="AbortError"||e.message.includes("AbortError"))))return;const a=(u=e==null?void 0:e.ruffleIndexError)!==null&&u!==void 0?u:Ee.Unknown,s=Object.assign([],{stackIndex:-1,avmStackIndex:-1});if(s.push(`# Error Info
`),e instanceof Error){if(s.push(`Error name: ${e.name}
`),s.push(`Error message: ${e.message}
`),e.stack){const Be=s.push(`Error stack:
\`\`\`
${e.stack}
\`\`\`
`)-1;if(e.avmStack){const Te=s.push(`AVM2 stack:
\`\`\`
    ${e.avmStack.trim().replace(/\t/g,"    ")}
\`\`\`
`)-1;s.avmStackIndex=Te}s.stackIndex=Be}}else s.push(`Error: ${e}
`);s.push(this.getPanicData());const g=s.join(""),L=new Date(Je.buildDate),K=new Date;K.setMonth(K.getMonth()-6);const ne=K>L;let ie,oe,me;if(ne)ie=new Ie(mu+"#downloads",we("update-ruffle"));else{let Be;Be=document.location.protocol.includes("extension")?this.swfUrl.href:document.location.href,Be=Be.split(/[?#]/,1)[0];let Te=`https://github.com/ruffle-rs/ruffle/issues/new?title=${encodeURIComponent(`Error on ${Be}`)}&template=error_report.md&labels=error-report&body=`,Ne=encodeURIComponent(g);s.stackIndex>-1&&String(Te+Ne).length>8195&&(s[s.stackIndex]=null,s.avmStackIndex>-1&&(s[s.avmStackIndex]=null),Ne=encodeURIComponent(s.join(""))),Te+=Ne,ie=new Ie(Te,we("report-bug"))}switch(a){case Ee.FileProtocol:oe=ye("error-file-protocol"),me=this.createErrorFooter([new Ie(mu+"/demo",we("ruffle-demo")),new Ie(mu+"#downloads",we("ruffle-desktop"))]);break;case Ee.JavascriptConfiguration:oe=ye("error-javascript-config"),me=this.createErrorFooter([new Ie("https://github.com/ruffle-rs/ruffle/wiki/Using-Ruffle#javascript-api",we("ruffle-wiki")),new Ie]);break;case Ee.WasmNotFound:oe=ye("error-wasm-not-found"),me=this.createErrorFooter([new Ie("https://github.com/ruffle-rs/ruffle/wiki/Using-Ruffle#configuration-options",we("ruffle-wiki")),new Ie]);break;case Ee.WasmMimeType:oe=ye("error-wasm-mime-type"),me=this.createErrorFooter([new Ie("https://github.com/ruffle-rs/ruffle/wiki/Using-Ruffle#configure-webassembly-mime-type",we("ruffle-wiki")),new Ie]);break;case Ee.SwfFetchError:oe=ye("error-swf-fetch"),me=this.createErrorFooter([new Ie]);break;case Ee.SwfCors:oe=ye("error-swf-cors"),me=this.createErrorFooter([new Ie("https://github.com/ruffle-rs/ruffle/wiki/Using-Ruffle#configure-cors-header",we("ruffle-wiki")),new Ie]);break;case Ee.WasmCors:oe=ye("error-wasm-cors"),me=this.createErrorFooter([new Ie("https://github.com/ruffle-rs/ruffle/wiki/Using-Ruffle#configure-cors-header",we("ruffle-wiki")),new Ie]);break;case Ee.InvalidWasm:oe=ye("error-wasm-invalid"),me=this.createErrorFooter([new Ie("https://github.com/ruffle-rs/ruffle/wiki/Using-Ruffle#addressing-a-compileerror",we("ruffle-wiki")),new Ie]);break;case Ee.WasmDownload:oe=ye("error-wasm-download"),me=this.createErrorFooter([new Ie]);break;case Ee.WasmDisabledMicrosoftEdge:oe=ye("error-wasm-disabled-on-edge"),me=this.createErrorFooter([new Ie("https://github.com/ruffle-rs/ruffle/wiki/Frequently-Asked-Questions-For-Users#edge-webassembly-error",we("more-info")),new Ie]);break;case Ee.JavascriptConflict:oe=ye("error-javascript-conflict"),ne&&oe.appendChild(ye("error-javascript-conflict-outdated",{buildDate:Je.buildDate})),me=this.createErrorFooter([ie,new Ie]);break;case Ee.CSPConflict:oe=ye("error-csp-conflict"),me=this.createErrorFooter([new Ie("https://github.com/ruffle-rs/ruffle/wiki/Using-Ruffle#configure-wasm-csp",we("ruffle-wiki")),new Ie]);break;default:oe=ye("error-unknown",{buildDate:Je.buildDate,outdated:String(ne)}),me=this.createErrorFooter([ie,new Ie])}const he=document.createElement("div");he.id="panic";const fe=document.createElement("div");fe.id="panic-title",fe.textContent=we("panic-title"),he.appendChild(fe);const de=document.createElement("div");de.id="panic-body",de.appendChild(oe),he.appendChild(de);const Se=document.createElement("div");Se.id="panic-footer",Se.appendChild(me),he.appendChild(Se),this.container.textContent="",this.container.appendChild(he);const Ve=this.container.querySelector("#panic-view-details");Ve&&(Ve.onclick=()=>{const Be=this.container.querySelector("#panic-body");Be.classList.add("details");const Te=document.createElement("textarea");return Te.readOnly=!0,Te.value=g,Be.replaceChildren(Te),!1}),this.destroy()}displayRootMovieDownloadFailedMessage(){var e,u,a;const s=(e=this.loadedConfig)===null||e===void 0?void 0:e.openInNewTab;if(s&&window.location.origin!==this.swfUrl.origin){const g=new URL(this.swfUrl);if(!((u=this.loadedConfig)===null||u===void 0)&&u.parameters){const oe=Zu((a=this.loadedConfig)===null||a===void 0?void 0:a.parameters);Object.entries(oe).forEach((([me,he])=>{g.searchParams.set(me,he)}))}this.hideSplashScreen();const L=document.createElement("div");L.id="message-overlay";const K=document.createElement("div");K.className="message",K.appendChild(ye("message-cant-embed"));const ne=document.createElement("div"),ie=document.createElement("a");ie.innerText=we("open-in-new-tab"),ie.onclick=()=>s(g),ne.appendChild(ie),K.appendChild(ne),L.appendChild(K),this.container.prepend(L)}else{const g=new Error("Failed to fetch: "+this.swfUrl);this.swfUrl.protocol.includes("http")?window.location.origin===this.swfUrl.origin||window.location.protocol.includes("extension")?g.ruffleIndexError=Ee.SwfFetchError:g.ruffleIndexError=Ee.SwfCors:g.ruffleIndexError=Ee.FileProtocol,this.panic(g)}}displayMessage(e){const u=document.createElement("div");u.id="message-overlay";const a=document.createElement("div");a.className="message";const s=document.createElement("p");s.textContent=e,a.appendChild(s);const g=document.createElement("div"),L=document.createElement("button");L.id="continue-btn",L.textContent=we("continue"),g.appendChild(L),a.appendChild(g),u.appendChild(a),this.container.prepend(u),this.container.querySelector("#continue-btn").onclick=()=>{u.parentNode.removeChild(u)}}displayUnsupportedVideo(e){const u=this.videoModal.querySelector("#video-holder");if(u){const a=document.createElement("video");a.addEventListener("contextmenu",(s=>s.stopPropagation())),a.src=e,a.autoplay=!0,a.controls=!0,u.textContent="",u.appendChild(a),this.videoModal.classList.remove("hidden")}}debugPlayerInfo(){return""}hideSplashScreen(){this.splashScreen.classList.add("hidden"),this.container.classList.remove("hidden")}showSplashScreen(){this.splashScreen.classList.remove("hidden"),this.container.classList.add("hidden")}setMetadata(e){this._metadata=e,this._readyState=bu.Loaded,this.hideSplashScreen(),this.dispatchEvent(new Event(Xe.LOADED_METADATA)),this.dispatchEvent(new Event(Xe.LOADED_DATA))}}function Rn(p,e){switch(p||(p="sameDomain"),p.toLowerCase()){case"always":return!0;case"never":return!1;default:try{return new URL(window.location.href).origin===new URL(e,window.location.href).origin}catch(u){return!1}}}function _n(p){return p===null||p.toLowerCase()==="true"}function Hu(p){if(p){let e="",u="";try{const a=new URL(p,mu);e=a.pathname,u=a.hostname}catch(a){}if(e.startsWith("/v/")&&/^(?:(?:www\.|m\.)?youtube(?:-nocookie)?\.com)|(?:youtu\.be)$/i.test(u))return!0}return!1}function Ju(p,e){var u,a;const s=p.getAttribute(e),g=(a=(u=window.RufflePlayer)===null||u===void 0?void 0:u.config)!==null&&a!==void 0?a:{};if(s)try{const L=new URL(s);L.protocol!=="http:"||window.location.protocol!=="https:"||"upgradeToHttps"in g&&g.upgradeToHttps===!1||(L.protocol="https:",p.setAttribute(e,L.toString()))}catch(L){}}function Vu(p){let e=p.parentElement;for(;e!==null;){switch(e.tagName){case"AUDIO":case"VIDEO":return!0}e=e.parentElement}return!1}Xe.LOADED_METADATA="loadedmetadata",Xe.LOADED_DATA="loadeddata",(function(p){p[p.HaveNothing=0]="HaveNothing",p[p.Loading=1]="Loading",p[p.Loaded=2]="Loaded"})(bu||(bu={}));class ot{constructor(e,u){this.isMuted=e,this.volume=u}get_volume(){return this.isMuted?0:this.volume/100}}class ou extends Xe{constructor(){super()}connectedCallback(){var e,u,a,s,g,L,K,ne,ie,oe,me,he,fe,de,Se,Ve,Be,Te,Ne,nu;super.connectedCallback();const Q=this.attributes.getNamedItem("src");if(Q){const ve=(u=(e=this.attributes.getNamedItem("allowScriptAccess"))===null||e===void 0?void 0:e.value)!==null&&u!==void 0?u:null,De=(s=(a=this.attributes.getNamedItem("menu"))===null||a===void 0?void 0:a.value)!==null&&s!==void 0?s:null;this.load({url:Q.value,allowScriptAccess:Rn(ve,Q.value),parameters:(L=(g=this.attributes.getNamedItem("flashvars"))===null||g===void 0?void 0:g.value)!==null&&L!==void 0?L:null,backgroundColor:(ne=(K=this.attributes.getNamedItem("bgcolor"))===null||K===void 0?void 0:K.value)!==null&&ne!==void 0?ne:null,base:(oe=(ie=this.attributes.getNamedItem("base"))===null||ie===void 0?void 0:ie.value)!==null&&oe!==void 0?oe:null,menu:_n(De),salign:(he=(me=this.attributes.getNamedItem("salign"))===null||me===void 0?void 0:me.value)!==null&&he!==void 0?he:"",quality:(de=(fe=this.attributes.getNamedItem("quality"))===null||fe===void 0?void 0:fe.value)!==null&&de!==void 0?de:"high",scale:(Ve=(Se=this.attributes.getNamedItem("scale"))===null||Se===void 0?void 0:Se.value)!==null&&Ve!==void 0?Ve:"showAll",wmode:(Te=(Be=this.attributes.getNamedItem("wmode"))===null||Be===void 0?void 0:Be.value)!==null&&Te!==void 0?Te:hu.Window,allowNetworking:(nu=(Ne=this.attributes.getNamedItem("allowNetworking"))===null||Ne===void 0?void 0:Ne.value)!==null&&nu!==void 0?nu:yu.All})}}get src(){var e;return(e=this.attributes.getNamedItem("src"))===null||e===void 0?void 0:e.value}set src(e){if(e){const u=document.createAttribute("src");u.value=e,this.attributes.setNamedItem(u)}else this.attributes.removeNamedItem("src")}static get observedAttributes(){return["src","width","height"]}attributeChangedCallback(e,u,a){var s,g,L,K;if(super.attributeChangedCallback(e,u,a),this.isConnected&&e==="src"){const ne=this.attributes.getNamedItem("src");ne&&this.load({url:ne.value,parameters:(g=(s=this.attributes.getNamedItem("flashvars"))===null||s===void 0?void 0:s.value)!==null&&g!==void 0?g:null,base:(K=(L=this.attributes.getNamedItem("base"))===null||L===void 0?void 0:L.value)!==null&&K!==void 0?K:null})}}static isInterdictable(e){const u=e.getAttribute("src"),a=e.getAttribute("type");return!!u&&!Vu(e)&&(Hu(u)?(Ju(e,"src"),!1):kn(u,a))}static fromNativeEmbedElement(e){const u=Nu("ruffle-embed",ou),a=document.createElement(u);return a.copyElement(e),a}}function Qe(p,e,u){e=e.toLowerCase();for(const[a,s]of Object.entries(p))if(a.toLowerCase()===e)return s;return u}function zn(p){var e,u;const a={};for(const s of p.children)if(s instanceof HTMLParamElement){const g=(e=s.attributes.getNamedItem("name"))===null||e===void 0?void 0:e.value,L=(u=s.attributes.getNamedItem("value"))===null||u===void 0?void 0:u.value;g&&L&&(a[g]=L)}return a}class su extends Xe{constructor(){super(),this.params={}}connectedCallback(){var e;super.connectedCallback(),this.params=zn(this);let u=null;this.attributes.getNamedItem("data")?u=(e=this.attributes.getNamedItem("data"))===null||e===void 0?void 0:e.value:this.params.movie&&(u=this.params.movie);const a=Qe(this.params,"allowScriptAccess",null),s=Qe(this.params,"flashvars",this.getAttribute("flashvars")),g=Qe(this.params,"bgcolor",this.getAttribute("bgcolor")),L=Qe(this.params,"allowNetworking",this.getAttribute("allowNetworking")),K=Qe(this.params,"base",this.getAttribute("base")),ne=Qe(this.params,"menu",null),ie=Qe(this.params,"salign",""),oe=Qe(this.params,"quality","high"),me=Qe(this.params,"scale","showAll"),he=Qe(this.params,"wmode","window");if(u){const fe={url:u};fe.allowScriptAccess=Rn(a,u),s&&(fe.parameters=s),g&&(fe.backgroundColor=g),K&&(fe.base=K),fe.menu=_n(ne),ie&&(fe.salign=ie),oe&&(fe.quality=oe),me&&(fe.scale=me),he&&(fe.wmode=he),L&&(fe.allowNetworking=L),this.load(fe)}}debugPlayerInfo(){var e;let u=`Player type: Object
`,a=null;return this.attributes.getNamedItem("data")?a=(e=this.attributes.getNamedItem("data"))===null||e===void 0?void 0:e.value:this.params.movie&&(a=this.params.movie),u+=`SWF URL: ${a}
`,Object.keys(this.params).forEach((s=>{u+=`Param ${s}: ${this.params[s]}
`})),Object.keys(this.attributes).forEach((s=>{var g;u+=`Attribute ${s}: ${(g=this.attributes.getNamedItem(s))===null||g===void 0?void 0:g.value}
`})),u}get data(){return this.getAttribute("data")}set data(e){if(e){const u=document.createAttribute("data");u.value=e,this.attributes.setNamedItem(u)}else this.attributes.removeNamedItem("data")}static isInterdictable(e){var u,a,s,g;if(Vu(e)||e.getElementsByTagName("ruffle-object").length>0||e.getElementsByTagName("ruffle-embed").length>0)return!1;const L=(u=e.attributes.getNamedItem("data"))===null||u===void 0?void 0:u.value.toLowerCase(),K=(s=(a=e.attributes.getNamedItem("type"))===null||a===void 0?void 0:a.value)!==null&&s!==void 0?s:null,ne=zn(e);let ie;if(L){if(Hu(L))return Ju(e,"data"),!1;ie=L}else{if(!ne||!ne.movie)return!1;if(Hu(ne.movie)){const me=e.querySelector("param[name='movie']");if(me){Ju(me,"value");const he=me.getAttribute("value");he&&e.setAttribute("data",he)}return!1}ie=ne.movie}const oe=(g=e.attributes.getNamedItem("classid"))===null||g===void 0?void 0:g.value.toLowerCase();return oe==="clsid:D27CDB6E-AE6D-11cf-96B8-444553540000".toLowerCase()?!Array.from(e.getElementsByTagName("object")).some(su.isInterdictable)&&!Array.from(e.getElementsByTagName("embed")).some(ou.isInterdictable):!oe&&kn(ie,K)}static fromNativeObjectElement(e){const u=Nu("ruffle-object",su),a=document.createElement(u);for(const s of Array.from(e.getElementsByTagName("embed")))ou.isInterdictable(s)&&s.remove();for(const s of Array.from(e.getElementsByTagName("object")))su.isInterdictable(s)&&s.remove();return a.copyElement(e),a}}class Sn{constructor(e){if(this.__mimeTypes=[],this.__namedMimeTypes={},e)for(let u=0;u<e.length;u++)this.install(e[u])}install(e){const u=this.__mimeTypes.length;this.__mimeTypes.push(e),this.__namedMimeTypes[e.type]=e,this[e.type]=e,this[u]=e}item(e){return this.__mimeTypes[e>>>0]}namedItem(e){return this.__namedMimeTypes[e]}get length(){return this.__mimeTypes.length}[Symbol.iterator](){return this.__mimeTypes[Symbol.iterator]()}}class st{constructor(e){this.__plugins=[],this.__namedPlugins={};for(let u=0;u<e.length;u++)this.install(e[u])}install(e){const u=this.__plugins.length;this.__plugins.push(e),this.__namedPlugins[e.name]=e,this[e.name]=e,this[u]=e}item(e){return this.__plugins[e>>>0]}namedItem(e){return this.__namedPlugins[e]}refresh(){}[Symbol.iterator](){return this.__plugins[Symbol.iterator]()}get length(){return this.__plugins.length}}const uu=new class extends Sn{constructor(p,e,u){super(),this.name=p,this.description=e,this.filename=u}}("Shockwave Flash","Shockwave Flash 32.0 r0","ruffle.js");var Ku,Gu;uu.install({type:xn,description:"Shockwave Flash",suffixes:"spl",enabledPlugin:uu}),uu.install({type:vn,description:"Shockwave Flash",suffixes:"swf",enabledPlugin:uu}),uu.install({type:gn,description:"Shockwave Flash",suffixes:"swf",enabledPlugin:uu}),uu.install({type:wn,description:"Shockwave Flash",suffixes:"swf",enabledPlugin:uu});const pu=(Gu=(Ku=window.RufflePlayer)===null||Ku===void 0?void 0:Ku.config)!==null&&Gu!==void 0?Gu:{},dt=z(pu)+"ruffle.js";let _u,zu,Su,Eu;function En(){var p,e;return(!("favorFlash"in pu)||pu.favorFlash!==!1)&&((e=(p=navigator.plugins.namedItem("Shockwave Flash"))===null||p===void 0?void 0:p.filename)!==null&&e!==void 0?e:"ruffle.js")!=="ruffle.js"}function jn(){try{_u=_u!=null?_u:document.getElementsByTagName("object"),zu=zu!=null?zu:document.getElementsByTagName("embed");for(const p of Array.from(_u))if(su.isInterdictable(p)){const e=su.fromNativeObjectElement(p);p.replaceWith(e)}for(const p of Array.from(zu))if(ou.isInterdictable(p)){const e=ou.fromNativeEmbedElement(p);p.replaceWith(e)}}catch(p){console.error(`Serious error encountered when polyfilling native Flash elements: ${p}`)}}function Cn(){Su=Su!=null?Su:document.getElementsByTagName("iframe"),Eu=Eu!=null?Eu:document.getElementsByTagName("frame"),[Su,Eu].forEach((p=>{for(const e of p){if(e.dataset.rufflePolyfilled!==void 0)continue;e.dataset.rufflePolyfilled="";const u=e.contentWindow,a=`Couldn't load Ruffle into ${e.tagName}[${e.src}]: `;try{u.document.readyState==="complete"&&An(u,a)}catch(s){w||console.warn(a+s)}e.addEventListener("load",(()=>{An(u,a)}),!1)}}))}async function An(p,e){var u;let a;await new Promise((s=>{window.setTimeout((()=>{s()}),100)}));try{if(a=p.document,!a)return}catch(s){return void(w||console.warn(e+s))}if(w||a.documentElement.dataset.ruffleOptout===void 0){if(w)p.RufflePlayer||(p.RufflePlayer={}),p.RufflePlayer.config=Object.assign(Object.assign({},pu),(u=p.RufflePlayer.config)!==null&&u!==void 0?u:{});else if(!p.RufflePlayer){const s=a.createElement("script");s.setAttribute("src",dt),s.onload=()=>{p.RufflePlayer={},p.RufflePlayer.config=pu},a.head.appendChild(s)}}}function ct(){En()||(function(p){"install"in navigator.plugins&&navigator.plugins.install||Object.defineProperty(navigator,"plugins",{value:new st(navigator.plugins),writable:!1}),navigator.plugins.install(p),!(p.length>0)||"install"in navigator.mimeTypes&&navigator.mimeTypes.install||Object.defineProperty(navigator,"mimeTypes",{value:new Sn(navigator.mimeTypes),writable:!1});const e=navigator.mimeTypes;for(let u=0;u<p.length;u+=1)e.install(p[u])})(uu)}function lt(){En()||(jn(),Cn(),new MutationObserver((function(p){p.some((e=>Array.from(e.addedNodes).some((u=>["EMBED","OBJECT"].includes(u.nodeName)||u instanceof Element&&u.querySelector("embed, object")!==null))))&&(jn(),Cn())})).observe(document,{childList:!0,subtree:!0}))}const In={version:Je.versionNumber+"+"+Je.buildDate.substring(0,10),polyfill(){lt()},pluginPolyfill(){ct()},createPlayer(){const p=Nu("ruffle-player",Xe);return document.createElement(p)}};class ju{constructor(e){var u;this.sources=(e==null?void 0:e.sources)||{},this.config=(e==null?void 0:e.config)||{},this.invoked=(e==null?void 0:e.invoked)||!1,this.newestName=(e==null?void 0:e.newestName)||null,(u=e==null?void 0:e.superseded)===null||u===void 0||u.call(e),document.readyState==="loading"?document.addEventListener("readystatechange",this.init.bind(this)):window.setTimeout(this.init.bind(this),0)}get version(){return"0.1.0"}registerSource(e){this.sources[e]=In}newestSourceName(){let e=null,u=te.fromSemver("0.0.0");for(const a in this.sources)if(Object.prototype.hasOwnProperty.call(this.sources,a)){const s=te.fromSemver(this.sources[a].version);s.hasPrecedenceOver(u)&&(e=a,u=s)}return e}init(){if(!this.invoked){if(this.invoked=!0,this.newestName=this.newestSourceName(),this.newestName===null)throw new Error("No registered Ruffle source!");(!("polyfills"in this.config)||this.config.polyfills)!==!1&&this.sources[this.newestName].polyfill()}}newest(){const e=this.newestSourceName();return e!==null?this.sources[e]:null}satisfying(e){const u=je.fromRequirementString(e);let a=null;for(const s in this.sources)if(Object.prototype.hasOwnProperty.call(this.sources,s)){const g=te.fromSemver(this.sources[s].version);u.satisfiedBy(g)&&(a=this.sources[s])}return a}localCompatible(){return this.sources.local!==void 0?this.satisfying("^"+this.sources.local.version):this.newest()}local(){return this.sources.local!==void 0?this.satisfying("="+this.sources.local.version):this.newest()}superseded(){this.invoked=!0}static negotiate(e,u){let a;return a=e instanceof ju?e:new ju(e),u!==void 0&&(a.registerSource(u),(!("polyfills"in a.config)||a.config.polyfills)!==!1&&In.pluginPolyfill()),a}}window.RufflePlayer=ju.negotiate(window.RufflePlayer,"local")})()})();
