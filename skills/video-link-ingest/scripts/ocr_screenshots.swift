#!/usr/bin/env swift

import AppKit
import Foundation
import Vision

struct OCRItem: Codable {
    let path: String
    let text: String
    let char_count: Int
}

func recognizeText(at imagePath: String) throws -> OCRItem {
    let url = URL(fileURLWithPath: imagePath)
    guard
        let image = NSImage(contentsOf: url),
        let cgImage = image.cgImage(forProposedRect: nil, context: nil, hints: nil)
    else {
        throw NSError(domain: "ocr_screenshots", code: 1, userInfo: [
            NSLocalizedDescriptionKey: "Unable to load image: \(imagePath)"
        ])
    }

    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = true
    request.recognitionLanguages = ["zh-Hans", "en-US"]

    let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
    try handler.perform([request])

    let lines = (request.results ?? [])
        .compactMap { observation in
            observation.topCandidates(1).first?.string.trimmingCharacters(in: .whitespacesAndNewlines)
        }
        .filter { !$0.isEmpty }

    let text = lines.joined(separator: "\n")
    return OCRItem(path: imagePath, text: text, char_count: text.count)
}

let paths = Array(CommandLine.arguments.dropFirst())
if paths.isEmpty {
    fputs("[]\n", stderr)
    exit(0)
}

do {
    let items = try paths.map { try recognizeText(at: $0) }
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted]
    let data = try encoder.encode(items)
    FileHandle.standardOutput.write(data)
} catch {
    fputs("\(error.localizedDescription)\n", stderr)
    exit(1)
}
